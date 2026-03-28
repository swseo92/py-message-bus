# AsyncRedisMessageBus: Visual Reference Guide

Quick visual guide to understanding the analysis.

---

## Current Architecture Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         AsyncRedisMessageBus                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  PRODUCER PATH (execute, publish, dispatch, send)                │
│  ────────────────────────────────────────────────                │
│   Message
│       │
│       ├─ _xadd() ──────┐
│       │                 │ Pipelined (1 RTT)
│       └─ SET NX EX ─────┘
│              │
│              └──> Redis Streams {app}:{type}:{name}
│                   └─ XADD (with maxlen trim)
│                   └─ SET {app}:dedup:* (idempotency)
│
│  CONSUMER PATH (background tasks)
│  ────────────────────────────────
│   Per-Type Consumer Loop
│       │
│       ├─ XREADGROUP ──────────> Fetch batch (count=10)
│       │   (1 RTT per batch)
│       │
│       ├─> For each message_id
│       │       │
│       │       ├─ _claim_or_ack_dedup() ──> Lua: SET NX + XACK
│       │       │  (1 RTT, atomicity)
│       │       │
│       │       ├─ Deserialize
│       │       │
│       │       ├─ Handler (user code)
│       │       │
│       │       ├─ Direct XACK ────────────> 1 RTT per message
│       │       │  (opportunity: batch)
│       │       │
│       │       └─ _release_dedup_key on error
│       │
│       └─ Background: XAUTOCLAIM (every 30s)
│           Recover messages in PEL
│
│  QUERY REPLY PATH (optimized to 2 RTT)
│  ──────────────────────────────────────
│   Caller (send)
│       │
│       ├─ Generate correlation_id
│       │
│       ├─ Register asyncio.Future
│       │
│       └─ XADD query stream ──────────────> 1 RTT
│              │
│              └─> Consumer picks up
│                   │
│                   ├─ Handler executes
│                   │
│                   └─ Pipeline PUBLISH + XACK ──> 1 RTT (pipelined)
│                          │
│                          └─> Shared PSUBSCRIBE listener
│                              │
│                              └─ Poll get_message() (10ms)
│                                  │
│                                  └─ Resolve Future
│                                      │
│                                      └─ Caller awaits
│
```

---

## RTT Comparison Matrix

```
┌──────────────────────────────────────────────────────────────────┐
│                         RTT COST ANALYSIS                        │
├──────────────────┬─────────┬──────────────┬─────────────────────┤
│ Operation        │ Current │ Optimized    │ Notes               │
├──────────────────┼─────────┼──────────────┼─────────────────────┤
│ XADD (w/dedup)   │ 1 RTT   │ 1 RTT        │ Already pipelined   │
│ XADD (no dedup)  │ 1 RTT   │ 1 RTT        │ Simple case         │
│                  │         │              │                     │
│ XREADGROUP       │ 1 RTT   │ 1 RTT        │ count=10→50 reduces │
│ (per call)       │ (10 msg)│ (50 msg)     │ frequency by 5x     │
│                  │         │              │                     │
│ XACK (per msg)   │ 1 RTT   │ 0.1 RTT      │ Batch 10 msgs in 1  │
│                  │ ×N msgs │ (amortized)  │ RTT = 90% savings   │
│                  │         │              │                     │
│ Lua dedup check  │ 1 RTT   │ 1 RTT        │ Saves vs 2 RTT      │
│ + XACK (dup)     │         │              │ (separate SET+XACK) │
│                  │         │              │                     │
│ Query reply      │ 2 RTT   │ 2 RTT        │ Pub/Sub optimized   │
│ (PUBLISH+XACK)   │         │ (+10ms poll) │ vs 4 RTT (streams)  │
└──────────────────┴─────────┴──────────────┴─────────────────────┘

Per-Message Cost Summary:
────────────────────────
Current:   XADD (1) + XREADGROUP (0.1) + dedup (0.1) + XACK (1) = ~2.2 RTT
Optimized: XADD (1) + XREADGROUP (0.02) + dedup (0.1) + XACK (0.1) = ~1.22 RTT
Gain:      ~45% RTT reduction = ~15-25% throughput gain
```

---

## Memory Growth at 12,000 msg/s

```
Dedup Keys (TTL = 86400 sec = 1 day):
──────────────────────────────────────
Time    Concurrent Keys    Memory Used
────    ───────────────    ──────────
1h      43.2M keys         ~2.2 GB
6h      259M keys          ~13 GB
12h     518M keys          ~26 GB
24h     1.04B keys         ~52 GB  ← Steady state

Without TTL:
┌──────────────────────┐
│ UNBOUNDED GROWTH!    │  ← CRITICAL: Always set TTL
│ (Memory exhaustion)  │
└──────────────────────┘

With 3600s (1 hour) TTL:
      Memory limited to ~2 GB (24x reduction)
      Risk: Messages > 1h old may be re-processed
```

---

## Throughput Scaling

```
Current Baseline: 12,000 msg/s

Optimization Impact:
────────────────────
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  Batch XACK (3)        ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  +15% = 13,800    │
│  Increase count (2)    ▓▓▓▓▓▓▓▓▓▓        +5% = 12,600     │
│  EVALSHA cache (1)     ▓▓               +0.5% negligible   │
│                                                             │
│  Combined (1+2+3):     ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓    │
│                        +20% = 14,400 msg/s                 │
│                                                             │
│  With compression (4): ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  │
│                        +25% = 15,000 msg/s                 │
│                        (plus -40% bandwidth)               │
│                                                             │
└─────────────────────────────────────────────────────────────┘

Implementation Cost vs Gain:
───────────────────────────
Quick Wins (1-2 hours):     +5% with count increase
Easy Wins (4-6 hours):      +15% with batch XACK
Medium Effort (6-8 hours):  +25% with compression

Expected ROI: High (effort) → High (gain)
```

---

## Optimization Priority Matrix

```
         HIGH IMPACT
             ▲
             │     Batch XACK ⭐
             │     +15% throughput
             │     Medium effort
             │
     Compression   ├─ Increase count ⭐
     +25% BW       │  +5% throughput
             │     │  1-line change
             │     │
             │     ├─ EVALSHA cache ⭐
             │     │  Negligible gain
             │     │  20-line change
             │
             │  Multi-stream     Monitoring
             │  XREADGROUP       + observability
             │
             └──────────────────────────▶
         LOW        EFFORT        HIGH

Recommended path (left to right):
  1. Increase count        (1h,  +5%, Low risk)
  2. EVALSHA caching       (1h,  +0.5%, Low risk)
  3. Batch XACK            (4h,  +15%, Medium risk)
  4. Compression (opt)     (6h,  +25% BW, Medium risk)
```

---

## Consumer Loop Topology

```
Single AsyncRedisMessageBus Instance:
─────────────────────────────────────

                    Redis Streams
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   command:*         event:*          query:*
        │                │                │
        │                │                │
    ┌───▼────┐       ┌───▼────┐      ┌───▼────┐
    │ Cmd    │       │ Event  │      │ Query  │
    │ Group  │       │Groups  │      │ Group  │
    │        │       │(per    │      │        │
    │ (shared)       │subscriber)    │(shared)│
    └───┬────┘       └───┬────┘      └───┬────┘
        │                │                │
    ┌───▼──────────┐ ┌───▼──────────┐ ┌──▼──────────┐
    │_run_single_  │ │_run_event_   │ │_run_query_  │
    │command_      │ │consumer()    │ │consumer()   │
    │consumer()    │ │(×N per sub)  │ │             │
    │              │ │              │ │             │
    │XREADGROUP    │ │XREADGROUP    │ │XREADGROUP   │
    │count=10      │ │count=10      │ │count=10     │
    │              │ │              │ │             │
    │Concurrent:   │ │Concurrent:   │ │Concurrent:  │
    │Semaphore     │ │Per-handler   │ │Sequential   │
    │(optional)    │ │(if config)   │ │or Semaphore │
    └───────┬──────┘ └───────┬──────┘ └─────┬───────┘
            │                │              │
            │                │              │
        ┌───▼────────────────▼──────────────▼────┐
        │   Background: XAUTOCLAIM loop (30s)    │
        │   - Claim idle messages from PEL       │
        │   - Route to DLQ if max_retry exceeded │
        └─────────────────────────────────────────┘

Key Features:
─────────────
✓ Per-type loops prevent HOL blocking
✓ Each consumer loop independent task
✓ Semaphore-based concurrency control (optional)
✓ XAUTOCLAIM recovers from crashes
✓ Per-subscriber groups for events (fan-out)
```

---

## Query Reply Path: Old vs New

```
OLD (Stream-based, 4 RTT):
─────────────────────────

Caller              Consumer         Redis
  │                   │                │
  ├─ XADD query ─────────────────────►│
  │                   │                │ stream: query_*
  │                   ├─ XREADGROUP ──►│
  │                   │◄───────────────┤
  │                   │                │
  │                   ├─ Handler exec  │
  │                   │                │
  │                   ├─ XADD reply ──►│
  │                   │                │ stream: reply_*
  │                   │                │
  │◄───── XREAD reply─┼────────────────┤
  │                   │                │
  │                   │                │
  └─────────────────────────────────────
  Latency: 4 RTT + handler + waiting


NEW (Pub/Sub, 2 RTT):
──────────────────

Caller              Consumer         Redis
  │                   │                │
  ├─ XADD query ─────────────────────►│
  │ (with reply_ch)   │                │ stream: query_*
  │                   │                │
  │ Creates Future    │                │
  │ Listens PSUBSCRIBE on reply_ch:*  │
  │                   │                │
  │                   ├─ XREADGROUP ──►│
  │                   │◄───────────────┤
  │                   │                │
  │                   ├─ Handler exec  │
  │                   │                │
  │                   ├─ Pipeline:     │
  │                   │  ├─ PUBLISH ──►│ reply_ch:correlation_id
  │                   │  └─ XACK ─────►│
  │                   │                │
  │◄───── get_message()────────────────┤ (polling 10ms)
  │ Resolve Future                     │
  │                   │                │
  └─────────────────────────────────────
  Latency: 2 RTT + handler + polling


RTT Comparison:
───────────────
Old: XADD (1) → XREADGROUP (1) → XADD reply (1) → XREAD (1) = 4 RTT
New: XADD (1) → XREADGROUP (1) → PUBLISH+XACK (0) = 2 RTT

Savings: 2 RTT ≈ 4-20 ms (depending on network)
Polling: ±10 ms (up to next 10ms poll cycle)
Net improvement: 4-20 ms → 0-10 ms (50-100% faster)
```

---

## Dedup Mechanism: Lua vs Separate

```
CURRENT: Lua Script (1 RTT)
───────────────────────────

Consumer                    Redis
   │                         │
   ├─ EVAL ack_and_dedup ───►│
   │  (atomically)           │
   │                         ├─ SET NX (dedup_key)
   │                         │
   │                         ├─ If duplicate:
   │                         │   XACK (auto)
   │                         │   return 0
   │                         │
   │                         ├─ If new:
   │                         │   SET NX success
   │                         │   return 1
   │                         │
   │◄────── result ──────────┤
   │
   ├─ if result == 1:
   │    Process message
   │    XACK manually
   │
   └─ if result == 0:
        Skip (dup, already ACK'd)

Total: 1 RTT (both branches)


ALTERNATIVE: Separate (2 RTT)
──────────────────────────────

Consumer                    Redis
   │                         │
   ├─ SET NX ────────────────►│
   │                         │ SET {dedup_key} 1 NX
   │◄────── OK/null ─────────┤
   │
   ├─ if OK:
   │    Process
   │    XACK ───────────────►│
   │                         │ XACK
   │                         │
   │◄────── OK ──────────────┤
   │
   └─ if null:
        Process?
        XACK ───────────────►│ (still ACK)


⚠️  Race window: SET returns null, but another consumer
    might process before we XACK → potential duplicate!

Lua eliminates this race: atomicity in one RTT
```

---

## Batch XACK Implementation Concept

```
Current: Per-Message ACK
────────────────────────

Message 1 ──────────► Handler ──────────► XACK ──► 1 RTT
                        ×                  ×
Message 2 ──────────► Handler ──────────► XACK ──► 1 RTT
                        ×                  ×
Message 3 ──────────► Handler ──────────► XACK ──► 1 RTT
                        ×                  ×
Message 4 ──────────► Handler ──────────► XACK ──► 1 RTT
                        ×                  ×
Message 5 ──────────► Handler ──────────► XACK ──► 1 RTT
                        ×                  ×

Total: 5 RTT for 5 messages = 1 RTT/msg


Optimized: Batch XACK
─────────────────────

Message 1 ──────────► Handler ──────┐
                                     │ Collect message_ids
Message 2 ──────────► Handler ──────┤
                                     │ Collect message_ids
Message 3 ──────────► Handler ──────┤
                                     │ Collect message_ids
Message 4 ──────────► Handler ──────┤
                                     │ Collect message_ids
Message 5 ──────────► Handler ──────┼──► Pipeline XACK ──► 1 RTT
                                     │    (all 5 messages)

Total: 1 RTT for 5 messages = 0.2 RTT/msg

Savings: 80% reduction in ACK RTTs


Tradeoff:
─────────
✓ 80% faster
✗ Defer ACKs until batch completion
✗ If crash between batch, messages may reprocess
✗ Must handle exceptions carefully
```

---

## Key Metrics Dashboard (Proposed)

```
┌─────────────────────────────────────────────────────────────┐
│          AsyncRedisMessageBus Health Dashboard              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ THROUGHPUT                                                  │
│   Current:     12,000 msg/s  ████████████████░░░░░░░  100% │
│   Target:      15,000 msg/s  ████████████████████░░░░       │
│                                                             │
│ LATENCY (Query Reply)                                       │
│   P50:         5ms           ████░░░░░░░░░░░░░░░░░░░░  50ms │
│   P95:         25ms          ████████░░░░░░░░░░░░░░░░       │
│   P99:         45ms          ██████████░░░░░░░░░░░░░░       │
│                                                             │
│ PEL GROWTH (Pending Entry List)                             │
│   Command:     15 messages   ████░░░░░░░░░░░░░░░░░░░       │
│   Event:       8 messages    ██░░░░░░░░░░░░░░░░░░░░░       │
│   Query:       2 messages    ░░░░░░░░░░░░░░░░░░░░░░░       │
│                                                             │
│ MEMORY (Redis)                                              │
│   Used:        4.2 GB        ████████░░░░░░░░░░░░░░░       │
│   Dedup Keys:  245M          ███████░░░░░░░░░░░░░░░░       │
│   Streams:     450MB         █░░░░░░░░░░░░░░░░░░░░░░       │
│                                                             │
│ HEALTH                                                      │
│   Redis:       🟢 Connected                                │
│   Consumers:   🟢 Running (4 loops + XAUTOCLAIM)          │
│   Pub/Sub:     🟢 Listening (reply pattern)               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Document Map

```
REDIS_OPTIMIZATION_ANALYSIS.md (1020 lines)
├── Section 1: Command Patterns (970 lines)
│   ├── 1.1 XADD (Producer)
│   ├── 1.2 XREADGROUP (Consumer)
│   ├── 1.3 XACK (Acknowledgment)
│   ├── 1.4 SET (Dedup Keys)
│   ├── 1.5 PUBLISH/PSUBSCRIBE (Query Reply)
│   ├── 1.6 XAUTOCLAIM (Recovery)
│   └── 1.7 Pipelining
├── Section 2: Lua Scripts
├── Section 3: Consumer Loops
├── Section 4: Serialization
├── Section 5: Memory Management
├── Section 6: Connection Handling
├── Section 7: Benchmarks
├── Section 8: Priorities
├── Section 9: Recommendations
├── Section 10: Code Examples
└── Section 11: Monitoring

OPTIMIZATION_SUMMARY.md (258 lines)
├── Quick Reference
├── 8 Areas Analyzed
├── Top 3 Recommendations ⭐
├── Detailed Breakdown
├── Implementation Roadmap
├── Risk Assessment
└── Document Statistics

ANALYSIS_VISUAL_REFERENCE.md (this file)
├── Architecture Flow
├── RTT Matrix
├── Memory Growth
├── Throughput Scaling
├── Optimization Matrix
├── Consumer Topology
├── Query Reply Paths
├── Dedup Mechanism
├── Batch XACK
└── Dashboard
```

