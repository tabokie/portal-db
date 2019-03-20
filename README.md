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

## Tech Overview

**portal-db** provides `GET`, `PUT`, `DELETE`, `SCAN` operations on in-memory data set. This specific workload demands a space-efficient, high-performance storage structure.

In this respect, portal-db proposes **HashTrie** as a hybrid data structure that leverages hashtable's query performance and trie's data ordering. HashTrie can dynamically transform between two different structures w.r.t. to data amount without serious data race.

Also, to provide transaction-level persistency for in-memory data, portal-db applies Snapshot + BinLog approach. Deamon thread periodically flush global snapshot onto disk, while binary log will be appended to `.bin` everytime an update is granted.

It's worth noticing that portal-db also sacrifices `very-fast-scan`, `fast-recovery` in pursuit of those features. In another word, portal-db is purely an attempt to reach satisfiable tradeoff for this specific workload.

## Benchmark
