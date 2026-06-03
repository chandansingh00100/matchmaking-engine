# 5v5 Real-Time Competitive Matchmaker

## Overview

A multi-threaded matchmaking engine written in Rust.

Features:

- 5v5 matchmaking
- MMR-based matching
- Time-based relaxation
- Atomic player reservation
- Optimal team balancing
- Lock-free metrics collection
- Concurrent worker pool

---

## Architecture

Modules:

types.rs
queue.rs
worker.rs
engine.rs
metrics.rs
balancer.rs

---

## Matching Algorithm

Players enter an MMR-ordered queue.

Workers:

1. Select anchor players
2. Search nearby MMR range
3. Reserve players atomically
4. Build 10-player pool
5. Balance teams
6. Emit match result

---

## Team Balancing

The engine enumerates all
C(10,5)=252
possible 5v5 partitions.

The partition with minimum average-MMR difference is selected.

---

## Complexity

Enqueue:
O(log n)

Candidate Search:
O(log n + k)

Balancing:
O(252)

---

## Simulation

50,000 players

Random MMR:
0–5000

Concurrent enqueue threads

Outputs:

- Matches created
- Average wait
- P50 wait
- P95 wait
- Throughput

---

## Run

Build:

cargo build --release

Run demo:

cargo run

Run simulation:

cargo run --release --bin load_test

Run tests:

cargo test
