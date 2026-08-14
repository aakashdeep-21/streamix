# Streamix — Kafka-like Message Broker: Plan

**Goal:** REST-based message broker (topics → partitions → append-only logs) used as shared infrastructure by our Spring Boot services.
**Delivery semantics: at-least-once** — a message can be redelivered if a consumer crashes/rebalances after processing but before committing; consumers must be idempotent. Ordering is guaranteed **within a partition only**.

## Core Components
| Component | Responsibility |
|---|---|
| `BrokerEngine` | Core facade wiring topics, groups, offsets; single entry point for the API layer |
| `TopicManager` | Create/list/delete topics; validates name + partition count |
| `Partition` (+ partition manager) | Append-only log per partition; assigns monotonic offsets atomically |
| `Partitioner` | `floorMod(hash(key), N)` when key present; per-topic round-robin counter when absent |
| Producer API | REST: single + batch publish; ack = `{partition, offset}` per message |
| Consumer API | REST: register/leave consumer, poll, commit offsets |
| `GroupCoordinator` | Group membership, round-robin partition→consumer assignment, rebalance on join/leave/session-timeout; poll doubles as heartbeat |
| `OffsetStore` | Committed offsets per (group, topic, partition); in-memory fetch positions reset to committed on rebalance/restart |
| Storage layer | `LogStorage` interface; v1 = append-only file per partition (reads from memory); in-memory impl for tests |

## Package Structure (base `com.example.streamix`)
```
api/       controllers, request/response DTOs, @ControllerAdvice error handler
core/      BrokerEngine, TopicManager, Partition, Partitioner, Message
group/     GroupCoordinator, ConsumerGroup, membership + assignment
offset/    OffsetStore
storage/   LogStorage interface, FileLogStorage (v1), InMemoryLogStorage (tests)
config/    BrokerProperties (data dir, retention, session timeout, batch/message limits)
```

## REST API (`/api/v1`)
| Endpoint | Purpose |
|---|---|
| `POST /topics` | Create topic `{name, partitions}`; duplicate → 409 |
| `GET /topics`, `GET /topics/{t}` | List topics / describe one (partitions, begin+end offsets) |
| `DELETE /topics/{t}` | Delete topic and its offsets/assignments |
| `POST /topics/{t}/messages` | Publish one `{key?, value, headers?}` → `{partition, offset}` |
| `POST /topics/{t}/messages/batch` | Publish many (non-transactional) → per-message acks |
| `POST /groups/{g}/consumers` | Register consumer `{consumerId, topics, sessionTimeoutMs?}` → triggers rebalance; duplicate live id → 409 |
| `DELETE /groups/{g}/consumers/{id}` | Leave group → rebalance |
| `GET /groups/{g}/consumers/{id}/messages?max=` | Poll assigned partitions from fetch position; empty list when caught up; acts as heartbeat |
| `POST /groups/{g}/consumers/{id}/offsets` | Commit `[{topic, partition, offset}]` — offset = next to read |
| `GET /groups/{g}/offsets?topic=` | Inspect committed offsets (ops/debugging) |
| `GET /actuator/health`, `/actuator/metrics` | Liveness + broker metrics (topic/message counts, group sizes, lag) |

## Key Decisions
- **Partitioning & ordering:** key-hash → fixed partition (per-key ordering); no key → round-robin. Offsets are per-partition monotonic longs; no cross-partition ordering.
- **Consumer groups:** assignment is broker-side — consumers just poll and the broker serves whatever partitions they currently own, so clients stay dumb. Each partition has exactly one owner per group; independent groups keep independent committed offsets. A consumer that misses `sessionTimeout` (no poll) is evicted → rebalance → new owner resumes from last commit ⇒ at-least-once.
- **Offsets:** poll advances an in-memory fetch position (so repeated polls don't loop); commit appends to a durable offsets journal. Consumer restart resumes from committed offset; broker restart replays logs + journal, so acked messages and commits survive.
- **Consumption modes:** same API serves both — (a) continuous polling at any interval (set `sessionTimeoutMs` > interval); (b) scheduled batch drain (e.g. 9 AM cron): register → poll until empty → commit → deregister. Committed offsets outlive membership, so absent consumers lose nothing between runs; both modes coexist on one topic via separate groups. Retention must exceed the slowest consumer's cadence.
- **Storage v1:** write-through append-only file per partition (JSON-lines) + offsets journal; reads served from memory; startup replay rebuilds state, truncating a torn final record. Durable enough for daily-batch consumers without segment/index complexity; `LogStorage` interface keeps the Phase 2 engine a drop-in.
- **Concurrency:** per-partition `ReadWriteLock` (exclusive append + offset assignment, concurrent reads); group membership mutations serialized per group; offsets in `ConcurrentHashMap`.
- **Retention:** messages are never deleted on consumption; a `@Scheduled` sweeper (Phase 2) trims by configurable max age/size and advances the partition's begin offset.
- **Errors:** 404 unknown topic/group/consumer; 409 duplicate topic/consumer; 400 invalid partition count or commit offset out of range; polling past the latest offset → 200 + empty batch. Uniform JSON error body via `@ControllerAdvice`.

## Setup Changes
- `pom.xml`: add `spring-boot-starter-web`, `spring-boot-starter-actuator`, `spring-boot-starter-validation`.
- Rename `demo` → base package `com.example.streamix`, `StreamixApplication`, app name `streamix`.

## Deployment
- **One artifact, two targets:** `./mvnw package` builds an executable JAR; Railway runs it via a multi-stage Dockerfile (JDK build → JRE runtime), EC2/any VM runs `java -jar` directly — zero code differences, target is pure packaging.
- **Railway (primary):** Dockerfile + `railway.json` (healthcheck `/actuator/health`, restart policy) in repo root; GitHub integration auto-deploys on push. `server.port=${PORT:8080}`; **Railway Volume mounted at the data dir is mandatory** — Railway's filesystem is ephemeral, so a deploy without a volume wipes logs + offsets.
- **12-factor config:** every runtime setting (port, data dir, retention, session timeout, limits) is a Spring property with an env-var override; no target-specific code paths.

## Phases
1. **Core broker (next):** topics, partitioning, single/batch publish, consumer groups + rebalance, poll/commit, actuator health/metrics, error handling; simple durability = append-only file per partition + offsets journal with startup replay; Dockerfile + `railway.json` deploy artifacts; unit + MockMvc tests (in-memory storage).
2. **Retention + full persistence:** rolling segment files, time/size trimming by deleting expired segments, offset→position index (bounded-memory reads from disk), configurable flush/fsync policy, offset snapshots; retention/lag metrics.
3. **Client library:** thin Java producer/consumer SDK (separate Maven module) wrapping the REST API — poll loop, heartbeats, commit helpers — so services don't hand-roll HTTP.
