#!/usr/bin/env python
import asyncio
import copy
import itertools
import os
import signal
import time
import traceback
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from http import HTTPStatus
from typing import Dict, List, Callable, Optional, Type, Union

import aiohttp
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

# yapf: disable
from tensorrt_llm.executor import CppExecutorError
from tensorrt_llm.llmapi.disagg_utils import (DisaggServerConfig,
                                              MetadataServerConfig,
                                              get_ctx_gen_server_urls)
from tensorrt_llm.logger import logger
from tensorrt_llm.serve.metadata_server import create_metadata_server
from tensorrt_llm.serve.openai_protocol import (ChatCompletionRequest,
                                                ChatCompletionResponse,
                                                CompletionRequest,
                                                CompletionResponse,
                                                DisaggregatedParams,
                                                ErrorResponse)
from tensorrt_llm.serve.router import KvCacheAwareRouter, create_router
from tensorrt_llm.version import __version__ as VERSION

# yapf: enale
TIMEOUT_KEEP_ALIVE = 10  # seconds.


@dataclass
class CachedOpaqueState:
    """Cached opaque state for hot path execution.
    
    Stores the stable connection information (opaque state) from context servers
    to enable parallel execution of context and generation phases.
    """
    opaque_state: str  # Base64 encoded opaque state
    ctx_request_id: int  # Context request ID for generation server
    timestamp: float  # Cache entry creation time
    hit_count: int = 0  # Number of cache hits for monitoring
    
    def is_valid(self, ttl_seconds: int) -> bool:
        """Check if cache entry is still valid based on TTL."""
        return (time.time() - self.timestamp) < ttl_seconds


class OpaqueStateCache:
    """Cache for context server opaque states to enable hot path execution.
    
    The opaque state contains stable connection information (CommState) and
    KV cache configuration (CacheState) that remains constant for a given
    context server deployment. This allows us to cache and reuse this
    information across requests, enabling parallel execution of context
    and generation phases.
    """
    
    def __init__(self, ttl_seconds: int = 3600, max_entries: int = 1000):
        """Initialize the opaque state cache.
        
        Args:
            ttl_seconds: Time-to-live for cache entries in seconds
            max_entries: Maximum number of cache entries to prevent unbounded growth
        """
        self._cache: Dict[str, CachedOpaqueState] = {}
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._enabled = os.getenv("TRTLLM_HOT_PATH_ENABLED", "0") == "1"
        
        if self._enabled:
            logger.info(f"Hot path enabled with opaque state cache (TTL: {ttl_seconds}s, max entries: {max_entries})")
    
    def get(self, ctx_server: str) -> Optional[CachedOpaqueState]:
        """Get cached opaque state for a context server.
        
        Args:
            ctx_server: Context server URL
            
        Returns:
            Cached opaque state if found and valid, None otherwise
        """
        if not self._enabled:
            return None
            
        cached = self._cache.get(ctx_server)
        if cached and cached.is_valid(self._ttl_seconds):
            cached.hit_count += 1
            logger.debug(f"Opaque state cache hit for {ctx_server}, hit count: {cached.hit_count}")
            return cached
        elif cached:
            # Remove expired entry
            del self._cache[ctx_server]
            logger.debug(f"Removed expired cache entry for {ctx_server}")
        return None
    
    def set(self, ctx_server: str, opaque_state: str, ctx_request_id: int):
        """Cache opaque state for a context server.
        
        Args:
            ctx_server: Context server URL
            opaque_state: Base64 encoded opaque state
            ctx_request_id: Context request ID
        """
        if not self._enabled:
            return
            
        # Implement simple LRU eviction if at capacity
        if len(self._cache) >= self._max_entries:
            # Remove oldest entry
            oldest_key = min(self._cache.keys(), 
                           key=lambda k: self._cache[k].timestamp)
            del self._cache[oldest_key]
            logger.debug(f"Evicted oldest cache entry: {oldest_key}")
        
        self._cache[ctx_server] = CachedOpaqueState(
            opaque_state=opaque_state,
            ctx_request_id=ctx_request_id,
            timestamp=time.time()
        )
        logger.info(f"Cached opaque state for {ctx_server}")
    
    def invalidate(self, ctx_server: str):
        """Remove cached entry for a context server (e.g., on error).
        
        Args:
            ctx_server: Context server URL to invalidate
        """
        if ctx_server in self._cache:
            del self._cache[ctx_server]
            logger.info(f"Invalidated opaque state cache for {ctx_server}")
    
    def get_stats(self) -> dict:
        """Get cache statistics for monitoring.
        
        Returns:
            Dictionary with cache statistics
        """
        if not self._enabled:
            return {"enabled": False}
            
        total_hits = sum(entry.hit_count for entry in self._cache.values())
        return {
            "enabled": True,
            "entries": len(self._cache),
            "total_hits": total_hits,
            "ttl_seconds": self._ttl_seconds,
            "max_entries": self._max_entries,
            "servers": {
                server: {
                    "hit_count": entry.hit_count,
                    "age_seconds": time.time() - entry.timestamp,
                    "is_valid": entry.is_valid(self._ttl_seconds)
                }
                for server, entry in self._cache.items()
            }
        }

class OpenAIDisaggServer:

    def __init__(self,
                 config: DisaggServerConfig,
                 req_timeout_secs: int = 180,
                 server_start_timeout_secs: int = 180,
                 metadata_server_cfg: Optional[MetadataServerConfig] = None,
                 metrics_interval_secs: int = 0):

        self.ctx_servers, self.gen_servers = get_ctx_gen_server_urls(config.server_configs)
        self.metadata_server = create_metadata_server(metadata_server_cfg)
        self.ctx_router = create_router(
            config.ctx_router_config, self.ctx_servers, metadata_server_cfg, self.metadata_server)
        self.gen_router = create_router(
            config.gen_router_config, self.gen_servers, metadata_server_cfg, self.metadata_server)
        self.conditional_disagg_config = config.conditional_disagg_config

        self.perf_metrics_max_requests = config.perf_metrics_max_requests
        if self.perf_metrics_max_requests > 0:
            # record corresponding keys of context and generation servers for perf metrics
            # (ctx_server, gen_server, ctx_request_id)
            self.perf_metrics_keys = deque(maxlen=self.perf_metrics_max_requests)
            self.perf_metrics_keys_lock = asyncio.Lock()
            # server_key -> {ctx_request_id: perf_metrics}
            self.server_perf_metrics: dict[str, dict[int, dict]] = {}
        else:
            self.perf_metrics_keys = None
            self.perf_metrics_keys_lock = None
            self.server_perf_metrics = None

        if config.max_retries < 0:
            raise ValueError(f"Max retries {config.max_retries} must be greater than or equal to 0")
        self.max_retries = config.max_retries
        # Metrics counters and synchronization
        self._metrics = {
            "ctx_total_requests": 0,
            "ctx_completed_requests": 0,
            "gen_total_requests": 0,
            "gen_completed_requests": 0,
        }
        self._metrics_lock = asyncio.Lock()
        self._metrics_task = None
        self.metrics_interval_secs = metrics_interval_secs

        logger.info(f"Server max retries: {self.max_retries}")

        if (len(self.gen_servers) == 0):
            raise ValueError("At least one generation server must be provided")

        if os.getenv("TRTLLM_DISAGG_BENCHMARK_GEN_ONLY") != "1" and len(self.ctx_servers) == 0:
            raise ValueError("At least one context server must be provided")

        if self.conditional_disagg_config is not None and \
                not isinstance(self.gen_router, KvCacheAwareRouter):
            raise ValueError("Generation router must be a KvCacheAwareRouter to enable conditional disaggregation")

        # Session will be initialized in lifespan
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Initialize opaque state cache for hot path execution
        cache_ttl = int(os.getenv("TRTLLM_OPAQUE_CACHE_TTL", "3600"))
        cache_max_entries = int(os.getenv("TRTLLM_OPAQUE_CACHE_MAX_ENTRIES", "1000"))
        self.opaque_cache = OpaqueStateCache(ttl_seconds=cache_ttl, max_entries=cache_max_entries)

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Create a persistent aiohttp ClientSession
            self.session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=0, limit_per_host=0, force_close=True),
                timeout=aiohttp.ClientTimeout(total=req_timeout_secs))

            logger.info("Waiting for context and generation servers to be ready")
            await self.wait_for_servers_ready(server_start_timeout_secs)

            if self.metadata_server:
                logger.info("Starting server monitoring via metadata service")
                await self.ctx_router.start_server_monitoring(metadata_server_cfg.refresh_interval)
                await self.gen_router.start_server_monitoring(metadata_server_cfg.refresh_interval)

            # Start periodic metrics logging
            if self.metrics_interval_secs > 0:
                self._metrics_task = asyncio.create_task(self._log_metrics_periodically(self.metrics_interval_secs))

            yield

            if self.metadata_server:
                logger.info("Stopping server monitoring via metadata service")
                await self.ctx_router.stop_server_monitoring()
                await self.gen_router.stop_server_monitoring()

            # Stop periodic metrics logging
            if self._metrics_task is not None:
                self._metrics_task.cancel()
                try:
                    await self._metrics_task
                except asyncio.CancelledError:
                    pass

            await self.session.close()  # Ensure session cleanup

        self.app = FastAPI(lifespan=lifespan)

        @self.app.exception_handler(RequestValidationError)
        async def validation_exception_handler(_, exc):
            return JSONResponse(status_code=400, content={"error": str(exc)})

        self.register_routes()

    async def _increment_metric(self, key: str, amount: int = 1):
        if self.metrics_interval_secs > 0:
            async with self._metrics_lock:
                self._metrics[key] += amount

    async def _get_metrics_snapshot(self):
        async with self._metrics_lock:
            return dict(self._metrics)

    async def _log_metrics_periodically(self, interval_seconds: int):
        try:
            while True:
                await asyncio.sleep(interval_seconds)
                snapshot = await self._get_metrics_snapshot()
                logger.info(
                    (
                        f"[Statistics] total_context_requests={snapshot['ctx_total_requests']}, completed_context_requests={snapshot['ctx_completed_requests']}, "
                        f"total_generation_requests={snapshot['gen_total_requests']}, completed_generation_requests={snapshot['gen_completed_requests']}"
                    )
                )
        except asyncio.CancelledError:
            pass

    @staticmethod
    def create_error_response(
            message: str,
            status_code: HTTPStatus = HTTPStatus.BAD_REQUEST) -> ErrorResponse:
        raise HTTPException(status_code=500, detail=f"Internal server error {message}")

    def register_routes(self):
        self.app.add_api_route("/health", self.health, methods=["GET"])
        self.app.add_api_route("/version", self.version, methods=["GET"])
        self.app.add_api_route("/perf_metrics", self.perf_metrics, methods=["GET"])
        self.app.add_api_route("/v1/completions",
                               self.openai_completion,
                               methods=["POST"])
        self.app.add_api_route("/v1/chat/completions",
                               self.openai_chat_completion,
                               methods=["POST"])
        self.app.add_api_route("/cache_stats", self.cache_stats, methods=["GET"])

    async def health(self) -> Response:
        return Response(status_code=200)

    async def version(self) -> JSONResponse:
        ver = {"version": VERSION}
        return JSONResponse(content=ver)
    
    async def cache_stats(self) -> JSONResponse:
        """Get opaque state cache statistics for monitoring hot path performance."""
        stats = self.opaque_cache.get_stats()
        return JSONResponse(content=stats)

    async def _add_perf_metrics_keys(self, ctx_server: str, gen_server: str, ctx_request_id: int):
        async with self.perf_metrics_keys_lock:
            self.perf_metrics_keys.append((ctx_server, gen_server, ctx_request_id))

    async def perf_metrics(self) -> JSONResponse:
        if self.perf_metrics_keys is None:
            return JSONResponse(content=[])

        perf_metrics = {}
        exc = None
        try:
            for server in self.ctx_servers + self.gen_servers:
                async with self.session.get(f"{server}/perf_metrics") as response:
                    server_perf_metrics = await response.json()
                    perf_metrics[server] = server_perf_metrics
        except Exception as e:
            # Keep the exception to raise it after saving perf metrics
            exc = e

        return_metrics = []
        async with self.perf_metrics_keys_lock:
            for server in perf_metrics:
                server_metrics = self.server_perf_metrics.setdefault(server, {})
                for request_perf_metrics in perf_metrics[server]:
                    ctx_request_id = request_perf_metrics.get("ctx_request_id", None)
                    if ctx_request_id is None:
                        continue
                    server_metrics[ctx_request_id] = request_perf_metrics

                if len(server_metrics) > self.perf_metrics_max_requests:
                    # Remove oldest requests and keep at most perf_metrics_max_requests
                    num_remove = len(server_metrics) - self.perf_metrics_max_requests
                    removed_keys = list(itertools.islice(server_metrics.keys(), num_remove))
                    for ctx_request_id in removed_keys:
                        server_metrics.pop(ctx_request_id)
            if exc is not None:
                raise exc

            remain_keys = []
            for ctx_server, gen_server, ctx_request_id in self.perf_metrics_keys:
                gen_perf_metrics = self.server_perf_metrics[gen_server].pop(ctx_request_id, None)
                if gen_perf_metrics is None:
                    # generation not finished
                    remain_keys.append((ctx_server, gen_server, ctx_request_id))
                    continue
                ctx_perf_metrics = self.server_perf_metrics[ctx_server].pop(ctx_request_id, None)
                return_metrics.append({
                    "ctx_server": ctx_server,
                    "gen_server": gen_server,
                    "ctx_perf_metrics": ctx_perf_metrics,
                    "gen_perf_metrics": gen_perf_metrics})
            self.perf_metrics_keys = deque(remain_keys, maxlen=self.perf_metrics_max_requests)

        return JSONResponse(content=return_metrics)

    async def merge_streaming_responses(self, ctx_response,
                                        gen_server: str,
                                        gen_req: Union[CompletionRequest, ChatCompletionRequest]):
        try:
            if ctx_response is not None and len(ctx_response.choices) != 1:
                raise ValueError("Context server did not return a single choice. This is not expected")

            #If request finished after first token not due to length, return right away and skip gen
            if ctx_response is not None and ctx_response.choices[0].finish_reason not in ["length", "not_finished"]:
                yield "data: [DONE]\n\n".encode('utf-8')
            else:
                # Then yield the generation responses
                await self._increment_metric("gen_total_requests")
                if isinstance(gen_req, CompletionRequest):
                    gen_response = await self.send_completion_request(gen_server, gen_req)
                elif isinstance(gen_req, ChatCompletionRequest):
                    gen_response = await self.send_chat_request(gen_server, gen_req)
                else:
                    raise TypeError("Invalid request type: {type(gen_req).__name__}")

                async for chunk in gen_response.body_iterator:
                    yield chunk
                await self._increment_metric("gen_completed_requests")

        finally:
            await self.gen_router.finish_request(gen_req)

    async def openai_completion(self, req: CompletionRequest) -> Response:
        try:
            if not isinstance(req.prompt, str):
                # Check if it's a list and contains integers
                if type(req.prompt) is list and len(req.prompt) == 1:
                    req.prompt = req.prompt[0]
                elif not isinstance(req.prompt, list) or not all(isinstance(x, int) for x in req.prompt):
                    raise ValueError("Disaggregated server currently only supports single string prompt or list of integers in request")

            return await self._send_disagg_request(req)

        except Exception as e:
            await self._handle_exception(e)

    async def openai_chat_completion(self, req: ChatCompletionRequest) -> Response:

        try:
            return await self._send_disagg_request(req)
        except Exception as e:
            await self._handle_exception(e)

    async def _handle_exception(self, exception):
        if isinstance(exception, CppExecutorError):
            logger.error(traceback.format_exc())
            signal.raise_signal(signal.SIGINT)
        elif isinstance(exception, HTTPException):
            raise exception  # Re-raise HTTP exceptions properly
        else:
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Internal server error {str(exception)}")

    async def _send_context_request(self, ctx_server: str, ctx_req: Union[CompletionRequest, ChatCompletionRequest]):

        ctx_req.disaggregated_params = DisaggregatedParams(request_type="context_only")
        ctx_req.stream = False
        ctx_req.stream_options = None

        logger.debug("Sending request to ctx server: %s", ctx_server)
        await self._increment_metric("ctx_total_requests")
        try:
            if isinstance(ctx_req, ChatCompletionRequest):
                ctx_response = await self.send_chat_request(ctx_server, ctx_req)
            else:
                assert isinstance(ctx_req, CompletionRequest)
                ctx_response = await self.send_completion_request(ctx_server, ctx_req)
        finally:
            await self.ctx_router.finish_request(ctx_req)
            await self._increment_metric("ctx_completed_requests")

        choices = ctx_response.choices
        if len(choices) > 1:
            raise ValueError("Disagg server returned more than one choice. This is currently not supported in disaggregated server.")
        if choices[0].disaggregated_params is None:
            raise ValueError("Context server did not return disaggregated params")

        return ctx_response

    async def _send_disagg_request(self, req: Union[CompletionRequest, ChatCompletionRequest]):
        """Main request routing with hot/cold path optimization.
        
        Hot path: Uses cached opaque state to execute context and generation in parallel
        Cold path: Traditional sequential execution with opaque state caching
        """
        gen_server = None
        need_ctx = False
        try:
            # Determine if need context server
            condition = self.conditional_disagg_config
            if condition is not None:
                assert isinstance(self.gen_router, KvCacheAwareRouter)
                # Query kv cache status and select a best gen_server.
                # The server is reserved for generation request
                gen_server, info = await self.gen_router.get_next_server(req)
                match_length = sum(info["matches"])
                total_length = sum(len(token_list) for token_list in info["token_lists"])
                if match_length == 0 or total_length - match_length > condition.max_local_prefill_length:
                    need_ctx = True
            elif os.getenv("TRTLLM_DISAGG_BENCHMARK_GEN_ONLY") == "1":
                # Hard-code first token, ctx_request_id for testing
                req.disaggregated_params = DisaggregatedParams(
                    request_type="generation_only",
                    first_gen_tokens=[7],
                    ctx_request_id=1,
                    encoded_opaque_state=None,
                    draft_tokens=None)
                # Since KV cache for prompt tokens will be uninitialized, need to ignore eos
                req.ignore_eos = True
            else:
                need_ctx = True

            if need_ctx:
                ctx_req = copy.deepcopy(req)
                ctx_server, _ = await self.ctx_router.get_next_server(ctx_req)
                
                # Check cache for hot path optimization
                cached_state = self.opaque_cache.get(ctx_server)
                
                if cached_state:
                    # HOT PATH: Execute context and generation in parallel using cached opaque state
                    logger.info(f"Using hot path for {ctx_server} (cache hit #{cached_state.hit_count})")
                    return await self._execute_hot_path(ctx_req, req, ctx_server, gen_server, cached_state)
                else:
                    # COLD PATH: Traditional sequential execution with caching
                    logger.debug(f"Using cold path for {ctx_server} (cache miss)")
                    return await self._execute_cold_path(ctx_req, req, ctx_server, gen_server)
            else:
                ctx_response = None

            # Generation-only path (no context needed)
            if gen_server is None:
                gen_server, _ = await self.gen_router.get_next_server(req)
            logger.debug("Sending request to gen server: %s", gen_server)

            if need_ctx and self.perf_metrics_keys is not None:
                asyncio.create_task(self._add_perf_metrics_keys(
                    ctx_server, gen_server, req.disaggregated_params.ctx_request_id))

            if not req.stream:
                try:
                    if isinstance(req, CompletionRequest):
                        gen_response = await self.send_completion_request(gen_server, req)
                    else:
                        assert isinstance(req, ChatCompletionRequest)
                        gen_response = await self.send_chat_request(gen_server, req)
                    return gen_response
                finally:
                    if gen_server is not None:
                        await self.gen_router.finish_request(req)
            else:
                # Return a streaming response
                return StreamingResponse(
                    self.merge_streaming_responses(ctx_response, gen_server, req),
                    media_type="text/event-stream"
                )
        except:
            if gen_server is not None:
                await self.gen_router.finish_request(req)
            raise
    
    async def _execute_hot_path(self, ctx_req, gen_req, ctx_server: str, gen_server: Optional[str], cached_state: CachedOpaqueState):
        """Hot path: Execute context and generation in parallel using cached opaque state.
        
        This enables significant latency reduction by allowing the generation server to
        pre-establish connections and prepare for KV cache reception while the context
        server is still processing.
        """
        if gen_server is None:
            gen_server, _ = await self.gen_router.get_next_server(gen_req)
        
        # Prepare generation request with cached opaque state
        gen_req.disaggregated_params = DisaggregatedParams(
            request_type="generation_only",
            ctx_request_id=cached_state.ctx_request_id,
            encoded_opaque_state=cached_state.opaque_state,
            first_gen_tokens=None,  # Will be populated by context response if needed
            draft_tokens=None
        )
        
        try:
            # Execute context and generation in parallel
            ctx_task = asyncio.create_task(self._send_context_request(ctx_server, ctx_req))
            
            # For streaming, we need to handle differently
            if gen_req.stream:
                # Wait for context to get first tokens, then stream generation
                ctx_response = await ctx_task
                return StreamingResponse(
                    self.merge_streaming_responses(ctx_response, gen_server, gen_req),
                    media_type="text/event-stream"
                )
            else:
                # Non-streaming: execute both in parallel
                if isinstance(gen_req, CompletionRequest):
                    gen_task = asyncio.create_task(self.send_completion_request(gen_server, gen_req))
                else:
                    gen_task = asyncio.create_task(self.send_chat_request(gen_server, gen_req))
                
                # Wait for generation (primary response)
                gen_response = await gen_task
                
                # Optionally wait for context to complete (for cleanup)
                try:
                    await asyncio.wait_for(ctx_task, timeout=1.0)
                except asyncio.TimeoutError:
                    logger.debug("Context task still running after generation completed")
                
                return gen_response
                
        except Exception as e:
            logger.warning(f"Hot path failed for {ctx_server}: {str(e)}, falling back to cold path")
            # Invalidate cache and fallback to cold path
            self.opaque_cache.invalidate(ctx_server)
            return await self._execute_cold_path(ctx_req, gen_req, ctx_server, gen_server)
        finally:
            if gen_server is not None:
                await self.gen_router.finish_request(gen_req)
    
    async def _execute_cold_path(self, ctx_req, gen_req, ctx_server: str, gen_server: Optional[str]):
        """Cold path: Traditional sequential execution with opaque state extraction and caching.
        
        This follows the original flow but extracts and caches the opaque state for
        future hot path execution.
        """
        # Execute context request first
        ctx_response = await self._send_context_request(ctx_server, ctx_req)
        
        if ctx_response is not None and len(ctx_response.choices) != 1:
            raise ValueError("Context server did not return a single choice. This is not expected")
        
        # Extract and cache opaque state for future hot path
        if (ctx_response and 
            ctx_response.choices and 
            ctx_response.choices[0].disaggregated_params and
            ctx_response.choices[0].disaggregated_params.encoded_opaque_state):
            
            disagg_params = ctx_response.choices[0].disaggregated_params
            self.opaque_cache.set(
                ctx_server,
                disagg_params.encoded_opaque_state,
                disagg_params.ctx_request_id
            )
            logger.debug(f"Cached opaque state for {ctx_server}")
        
        # Continue with generation request (original logic)
        gen_req.disaggregated_params = ctx_response.choices[0].disaggregated_params
        gen_req.disaggregated_params.request_type = "generation_only"
        
        # Replace the string prompt with prompt_tokens_ids
        if isinstance(gen_req, CompletionRequest):
            gen_req.prompt = ctx_response.prompt_token_ids
        elif isinstance(gen_req, ChatCompletionRequest):
            gen_req.prompt_token_ids = ctx_response.prompt_token_ids
        else:
            raise ValueError(f"Invalid request type: {type(gen_req).__name__}")
        
        # Pick a generation server if not already selected
        if gen_server is None:
            gen_server, _ = await self.gen_router.get_next_server(gen_req)
        logger.debug("Sending request to gen server: %s", gen_server)
        
        if not gen_req.stream:
            try:
                # If request finished after first token for reason other than length, return right away
                if ctx_response is not None and ctx_response.choices[0].finish_reason not in ["length", "not_finished"]:
                    del ctx_response.choices[0].disaggregated_params
                    return ctx_response
                else:
                    if isinstance(gen_req, CompletionRequest):
                        gen_response = await self.send_completion_request(gen_server, gen_req)
                    else:
                        assert isinstance(gen_req, ChatCompletionRequest)
                        gen_response = await self.send_chat_request(gen_server, gen_req)
                    return gen_response
            finally:
                if gen_server is not None:
                    await self.gen_router.finish_request(gen_req)
        else:
            # Return a streaming response that combines both context and generation responses
            return StreamingResponse(
                self.merge_streaming_responses(ctx_response, gen_server, gen_req),
                media_type="text/event-stream"
            )


    async def __call__(self, host, port):
        config = uvicorn.Config(self.app,
                                host=host,
                                port=port,
                                log_level="info",
                                timeout_keep_alive=TIMEOUT_KEEP_ALIVE)
        await uvicorn.Server(config).serve()

    async def create_generator(self, url: str, request: Union[CompletionRequest, ChatCompletionRequest], end_point: str):
        async with self.session.post(url + end_point, json=request.model_dump(exclude_unset=True)) as response:
            content_type = response.headers.get("Content-Type", "")
            if "text/event-stream" in content_type:
                if not request.stream:
                    raise ValueError("Received an event-stream although request stream was False")

                try:
                    async for line in response.content.iter_any():
                        if line:
                            yield line
                            await asyncio.sleep(0)
                except Exception as e:
                    logger.error(f"Unexpected error in stream: {e}")
                    raise

    async def create_completion_generator(self, url: str, request: CompletionRequest):
        async for chunk in self.create_generator(url, request, "/v1/completions"):
            yield chunk

    async def create_chat_generator(self, url: str, request: ChatCompletionRequest):
        async for chunk in self.create_generator(url, request, "/v1/chat/completions"):
            yield chunk

    async def send_request(self, url: str,
                           request: Union[CompletionRequest, ChatCompletionRequest],
                           endpoint: str,
                           response_type: Type[Union[CompletionResponse, ChatCompletionResponse]],
                           create_generator: Callable) -> Union[CompletionResponse, ChatCompletionResponse, StreamingResponse]:
        for attempt in range(self.max_retries + 1):
            try:
                if request.stream:
                    response_generator = create_generator(url, request)
                    return StreamingResponse(content=response_generator, media_type="text/event-stream")
                else:
                    async with self.session.post(url + endpoint, json=request.model_dump(exclude_unset=True)) as response:
                        content_type = response.headers.get("Content-Type", "")
                        if "text/event-stream" in content_type:
                            raise ValueError("Received an event-stream although request stream was False")

                        response_dict = await response.json()
                        if not response.ok:
                            logger.error(f"Received failed response {response_dict}")
                            response.raise_for_status()
                        return response_type(**response_dict)
            except (aiohttp.ClientError, OSError) as e:
                if attempt == self.max_retries:
                    raise HTTPException(status_code=HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal server error") from e
                logger.error(f"Client error: {e} - retry {attempt} of {self.max_retries}")
                # TODO : add a configurable retry interval
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error encountered while processing request to {url+endpoint}: {e}")
                raise


    async def send_completion_request(self, url: str, request: CompletionRequest) -> Union[CompletionResponse, StreamingResponse]:
        return await self.send_request(url, request, "/v1/completions", CompletionResponse, self.create_completion_generator)

    async def send_chat_request(self, url: str, request: ChatCompletionRequest) -> ChatCompletionResponse:
        return await self.send_request(url, request, "/v1/chat/completions", ChatCompletionResponse, self.create_chat_generator)

    @classmethod
    async def check_server_ready(cls, session: aiohttp.ClientSession, server_url: str) -> bool:
        try:
            async with session.get(server_url+"/health") as response:
                return response.status == 200
        except Exception:
            return False

    @classmethod
    async def wait_for_all_servers_ready(cls, session: aiohttp.ClientSession,
                                         ctx_servers: list[str],
                                         gen_servers: list[str],
                                         server_start_timeout_secs: int = 180):
        async def get_unready_servers(servers: list[str]) -> list[str]:
            servers_ready = await asyncio.gather(*[cls.check_server_ready(session, server) for server in servers])
            return [server for server, ready in zip(servers, servers_ready) if not ready]

        async def check_all_servers_ready():
            iter = 0
            unready_servers = await get_unready_servers(ctx_servers + gen_servers)
            while len(unready_servers) > 0:
                wait_time = 3
                logger.info(
                    f"[{iter}] Servers are not ready. Waiting for {unready_servers}..."
                )
                await asyncio.sleep(wait_time)
                iter += 1
                unready_servers = await get_unready_servers(unready_servers)
        try:
            await asyncio.wait_for(check_all_servers_ready(), timeout=server_start_timeout_secs)
        except asyncio.CancelledError:
            raise TimeoutError("Timeout waiting for context and generation servers to be ready")
        logger.info("Context and generation servers are ready")

    async def wait_for_servers_ready(self, server_start_timeout_secs: int = 180):
        await self.wait_for_all_servers_ready(self.session, self.ctx_servers, self.gen_servers, server_start_timeout_secs)
