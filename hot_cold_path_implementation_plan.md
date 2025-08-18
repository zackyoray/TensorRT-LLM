# Hot/Cold Path Implementation Plan for Early Responding POC

## Overview

This approach leverages the fact that context servers have **stable connection information** (opaque state) that can be cached and reused across requests. This enables parallel execution of context and generation phases without complex orchestration changes.

## Key Insight

The opaque state contains:
- **CommState**: MPI ranks, IP:ports, NIXL agent info (stable per server)
- **CacheState**: KV cache configuration (stable per model/server deployment)

These values remain constant for a context server deployment, making them perfect for caching!

## Implementation Approach

### 1. Cold Path (First Request to a Context Server)
```
Client → Orchestrator → Context Server → Generation Server → Client
          ↓
        Cache opaque state
```

### 2. Hot Path (Subsequent Requests)
```
Client → Orchestrator → [Parallel] → Context Server
                    ↓               → Generation Server (with cached opaque)
                  Cache Hit                    ↓
                                          Early connection
```

## Minimal Code Changes Required

### Phase 1: Basic Hot/Cold Path (2-3 days)

#### 1.1 Add Opaque State Cache
```python
# In openai_disagg_server.py
class OpaqueStateCache:
    """Cache opaque states per context server"""
    def get(self, ctx_server: str) -> Optional[CachedOpaqueState]
    def set(self, ctx_server: str, opaque_state: str, ctx_request_id: int)
    def invalidate(self, ctx_server: str)
```

#### 1.2 Modify `_send_disagg_request`
- Check cache before sending context request
- Branch to hot path (parallel) or cold path (sequential)
- Extract and cache opaque state in cold path

#### 1.3 Environment Variables
```bash
TRTLLM_HOT_PATH_ENABLED=1          # Enable hot path
TRTLLM_OPAQUE_CACHE_TTL=3600      # Cache TTL in seconds
TRTLLM_HOT_PATH_DEBUG=1           # Debug logging
```

### Phase 2: Early Responding Integration (1-2 days)

#### 2.1 Generation Server Pre-warming
In hot path, generation server can:
- Receive opaque state immediately
- Pre-establish NIXL connections
- Wait for KV cache from context server

#### 2.2 Context Executor Changes
- Start early responding as designed
- KV cache transfer begins before GPU completes
- Generation server already connected and waiting

### Phase 3: Production Hardening (2-3 days)

#### 3.1 Monitoring
- Cache hit/miss rates
- Hot path success rate
- Latency improvements
- Fallback frequency

#### 3.2 Error Handling
- Graceful fallback to cold path
- Cache invalidation on errors
- Connection validation

## Benefits

### 1. **Minimal Changes**
- ~100 lines in orchestrator
- No changes to context/generation servers initially
- Backward compatible

### 2. **Immediate Value**
- Hot path reduces latency by enabling parallel execution
- Works even without early responding changes
- Early responding makes it even better

### 3. **Safe Rollout**
- Feature flag controlled
- Automatic fallback
- No risk to existing flow

## Performance Impact

### Without Early Responding
- **Cold Path**: Context(100ms) + Generation(200ms) = 300ms
- **Hot Path**: max(Context(100ms), Generation(200ms)) = 200ms
- **Improvement**: 33%

### With Early Responding
- **Hot Path**: max(Context(100ms), Generation_with_early_KV(150ms)) = 150ms
- **Improvement**: 50%

## Testing Strategy

### 1. Unit Tests
```python
def test_opaque_cache_operations()
def test_hot_path_execution()
def test_cold_path_fallback()
def test_cache_expiration()
```

### 2. Integration Tests
- End-to-end hot/cold path flows
- Cache persistence across requests
- Error scenarios and fallbacks

### 3. Performance Tests
```bash
# Baseline (hot path disabled)
python benchmark.py --hot-path-enabled=false

# With hot path
python benchmark.py --hot-path-enabled=true

# Compare latencies
```

## Deployment Plan

### Week 1
1. Implement OpaqueStateCache
2. Add hot/cold path logic
3. Basic testing

### Week 2  
1. Add monitoring
2. Error handling
3. Performance validation

### Week 3
1. Production deployment (disabled)
2. Gradual rollout
3. Monitor metrics

## Configuration Examples

### Development
```yaml
hot_path:
  enabled: true
  cache_ttl_seconds: 300
  debug_logging: true
  fallback_on_error: true
```

### Production
```yaml
hot_path:
  enabled: true
  cache_ttl_seconds: 3600
  debug_logging: false
  fallback_on_error: true
  max_cache_entries: 1000
```

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|---------|------------|
| Stale cache | Failed requests | TTL + invalidation on error |
| Memory growth | OOM | Max cache size limit |
| Race conditions | Incorrect state | Request ID validation |
| Network issues | Failed parallel exec | Automatic fallback |

## Success Metrics

1. **Cache Hit Rate**: >80% after warm-up
2. **Hot Path Success**: >95% of attempts
3. **Latency Reduction**: 25-35% average
4. **Error Rate**: No increase
5. **Memory Usage**: <100MB for cache

## Evolution Path

1. **Phase 1**: Basic hot/cold path (this POC)
2. **Phase 2**: Add early responding
3. **Phase 3**: Predictive pre-warming
4. **Phase 4**: Multi-level caching

## Summary

This hot/cold path approach with opaque state caching provides:
- **Immediate latency benefits** through parallel execution
- **Minimal code changes** (~100 lines)
- **Safe deployment** with automatic fallback
- **Foundation for early responding** optimization

The key insight is that opaque states are stable and cacheable, enabling us to break the sequential dependency between context and generation phases with minimal risk.
