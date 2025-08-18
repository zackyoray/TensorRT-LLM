# Hot/Cold Path Optimization for Disaggregated Serving

## Overview

This document describes the hot/cold path optimization implemented in TensorRT-LLM's disaggregated serving architecture. This optimization enables parallel execution of context and generation phases by caching stable connection information (opaque state) from context servers.

## Architecture

### Traditional Sequential Flow (Before)
```
Client → Orchestrator → Context Server → Wait → Generation Server → Response
                         (100ms)                    (200ms)
                    Total Latency: 300ms
```

### Optimized Parallel Flow (After)
```
Client → Orchestrator → [Cache Hit] → Context Server ─┐
                                  └→ Generation Server ┴→ Response
                                       (Parallel: max 200ms)
                    Total Latency: 200ms (33% reduction)
```

## Key Concepts

### Opaque State
The opaque state contains stable connection information that remains constant for a given context server:
- **CommState**: MPI ranks, IP:ports, NIXL agent information
- **CacheState**: KV cache configuration (heads, block size, TP/PP degrees)

### Hot Path
When the orchestrator has cached opaque state for a context server:
1. Check cache for context server's connection info
2. Start context and generation requests in parallel
3. Generation server can pre-establish connections while context processes

### Cold Path
First request to a context server or after cache miss:
1. Execute context request (sequential)
2. Extract and cache opaque state from response
3. Continue with generation request
4. Future requests can use hot path

## Implementation Details

### Cache Implementation
```python
@dataclass
class CachedOpaqueState:
    opaque_state: str      # Base64 encoded connection info
    ctx_request_id: int    # Context request identifier
    timestamp: float       # For TTL management
    hit_count: int = 0     # Monitoring metrics
```

### Configuration
Enable hot path optimization with environment variables:
```bash
# Enable hot path (default: disabled for backward compatibility)
export TRTLLM_HOT_PATH_ENABLED=1

# Cache TTL in seconds (default: 3600)
export TRTLLM_OPAQUE_CACHE_TTL=3600

# Maximum cache entries (default: 1000)
export TRTLLM_OPAQUE_CACHE_MAX_ENTRIES=1000
```

### Monitoring
Access cache statistics via HTTP endpoint:
```bash
curl http://localhost:8000/cache_stats
```

Response example:
```json
{
  "enabled": true,
  "entries": 5,
  "total_hits": 150,
  "ttl_seconds": 3600,
  "max_entries": 1000,
  "servers": {
    "http://ctx-server-1:8000": {
      "hit_count": 45,
      "age_seconds": 120.5,
      "is_valid": true
    }
  }
}
```

## Performance Benefits

### Latency Reduction
- **Sequential (Cold Path)**: Context(100ms) + Generation(200ms) = 300ms
- **Parallel (Hot Path)**: max(Context(100ms), Generation(200ms)) = 200ms
- **Improvement**: 33% latency reduction

### With Early Responding (Future)
When combined with early responding optimization:
- **Hot Path + Early Responding**: ~150ms (50% reduction possible)

## Error Handling

The implementation includes robust error handling:
1. **Automatic Fallback**: On hot path failure, automatically falls back to cold path
2. **Cache Invalidation**: Failed entries are removed from cache
3. **Graceful Degradation**: System continues to work even if caching fails
4. **Comprehensive Logging**: Detailed logs for debugging

## Deployment Guide

### 1. Enable in Development
```bash
# Start with hot path disabled (default)
python launch_disaggregated_server.py

# Enable after validation
export TRTLLM_HOT_PATH_ENABLED=1
python launch_disaggregated_server.py
```

### 2. Monitor Performance
```python
# Check cache effectiveness
import requests
stats = requests.get("http://localhost:8000/cache_stats").json()
hit_rate = stats["total_hits"] / (stats["total_hits"] + stats["entries"])
print(f"Cache hit rate: {hit_rate:.2%}")
```

### 3. Tune Parameters
```bash
# For stable deployments with long-lived servers
export TRTLLM_OPAQUE_CACHE_TTL=7200  # 2 hours

# For dynamic environments
export TRTLLM_OPAQUE_CACHE_TTL=600   # 10 minutes
```

## Troubleshooting

### Cache Not Working
1. Check if hot path is enabled: `TRTLLM_HOT_PATH_ENABLED=1`
2. Verify cache stats endpoint: `/cache_stats`
3. Look for "Using hot path" messages in logs

### High Cache Miss Rate
1. Check if context servers are stable (not restarting)
2. Increase TTL if servers are long-lived
3. Monitor for errors causing cache invalidation

### Performance Not Improving
1. Ensure requests are going to same context servers
2. Check that generation phase is actually the bottleneck
3. Verify parallel execution in logs

## Future Enhancements

This hot/cold path optimization provides the foundation for:
1. **Early Responding**: KV cache transfer before context completion
2. **Connection Pre-warming**: Proactive connection establishment
3. **Predictive Caching**: ML-based cache management
4. **Multi-level Caching**: Hierarchical cache for large deployments

## Code References

- Implementation: `tensorrt_llm/serve/openai_disagg_server.py`
- Cache class: `OpaqueStateCache`
- Hot path method: `_execute_hot_path()`
- Cold path method: `_execute_cold_path()`

## Related Documents

- [Disaggregated Architecture Overview](disaggregated_trtllm_nixl_architecture.md)
- [Opaque State Sequence Diagram](disaggregated_opaque_state_sequence.md)
- [Early Responding Architecture](context_executor_early_responding_architecture.md)
