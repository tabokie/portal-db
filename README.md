# portal-db

persistent and scalable in-memory key-value engine. **PingCAP** internship homework.

## Todo

- Server Front-end
	-	[x] socket \* thread
	-	[ ] socket \* multiplexing
  -	[ ] chunk recovery from break
- Query Dispatcher
  - [ ] range dispatch and sharding
- Storage Engine
  - [x] HashTrie
  - [x] `SCAN` operation and iterator
  -	[x] read-write thread safety
  - [ ] lock-free thread safety
- Durability
  - [x] snapshot and recovery
  - [ ] bin-log and recovery
  - [ ] thread safety
  - [ ] persist benchmark

## Feature

- **in-memory**: guarantee fast update and query unless key-value data exceeds memory capacity
- **persistent**: provide different level of persistency (best-effort, transaction-level)
- **consistent**: consistent `GET` / `PUT` and optional snapshot semantics for `SCAN` operation
- **scalable**: support range sharding

## Tech Overview

![architecture](./docs/arch.png)

**portal-db** provides `GET`, `PUT`, `DELETE`, `SCAN` operations on in-memory data set. This specific workload demands a space-efficient, high-performance storage structure.

In this respect, portal-db proposes **HashTrie** as a hybrid data structure that leverages hashtable's query performance and trie's data ordering. HashTrie can dynamically transform between two different structures w.r.t. to data amount without serious data race.

Also, to provide transaction-level persistency for in-memory data, portal-db applies Snapshot + BinLog approach. Deamon thread periodically flush global snapshot onto disk, while binary log will be appended to `.bin` everytime an update is granted.

It's worth noticing that portal-db also sacrifices `very-fast-scan`, `fast-recovery` in pursuit of those features. In another word, portal-db is purely an attempt to reach satisfiable tradeoff for this specific workload.

## Benchmark

- Setup

```
CPU         :   Core i5-6200U @ 2.30GHz
Memory      :   2 GB
Keys        :   8 bytes
Values      :   256 bytes
Entries     :   100'0000
```

- In-Memory HashTrie

```
single thread:
write       :   2.09354 seconds, 120.26 MB/s
read        :   1.37388 seconds, 183.25 MB/s
scan-sort   :   1.65211 seconds, 152.38 MB/s
scan-unsort :   1.33734 seconds, 188.26 MB/s
```

- Persistent HashTrie