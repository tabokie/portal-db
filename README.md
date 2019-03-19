# portal-db

persistent and scalable in-memory key-value engine. **PingCAP** internship homework.

## Todo

- [ ] Server Front-end
  - [ ] Customizable Protocol Layer
  - [ ] Basic Socket Layer
- [ ] Query Dispatcher
  - [ ] Hash Dispatch
  - [ ] Range Dispatch
  - [ ] Table Dispatch
- [ ] Storage Engine
  - [ ] Volatile Storage
  - [ ] query speed-up
  - [ ] persist engine

## Feature

-	**in-memory**: guarantee fast update and query unless key-value data exceeds memory capacity
-	**persistent**: provide different level of persistency (best-effort, transaction-level)
-	**consistent**: consistent `GET` / `PUT` and optional snapshot semantics for `SCAN` operation
-	**scalable**: support range sharding

## Tech Detail

**portal-db** provides `GET`, `PUT`, `DELETE`, `SCAN` operations on in-memory data set. This specific workload demands a space-efficient, ordered storage structure.

## Benchmark
