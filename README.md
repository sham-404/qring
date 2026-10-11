# qring

A high-performance, lock-free, zero-allocation single-producer single-consumer (SPSC) queue for Rust, with cache-aligned atomics.

`qring` is a fixed-capacity bounded queue built on a power-of-two ring buffer. It is `#![no_std]`, dependency-free, and designed for low-latency, high-throughput data paths such as IPC channels, audio processing, and embedded/real-time systems where per-operation allocation and locks are unacceptable.

## Features

- **Lock-free** — wait-free `push`/`pop` built on acquire/release atomics.
- **Zero-allocation** — the backing storage is inline; no heap, no allocator.
- **`no_std`** — usable on bare metal and embedded targets.
- **Cache-aligned** — producer and consumer indices are padded to 64-byte cache lines to eliminate false sharing between cores.
- **Compile-time capacity** — capacity is a const generic, and non-power-of-two sizes fail compilation.
- **Bulk operations** — `push_slice`/`pop_slice` move many elements at once with wraparound-aware `copy_nonoverlapping`.
- **All-or-nothing variants** — `push_exact`/`pop_exact` succeed fully or leave the queue untouched.
- **Sound ownership** — `split` hands out exactly one `Producer` and one `Consumer`; each may be moved to a different thread.
- **Correct cleanup** — dropping the queue runs the destructor of every unconsumed element exactly once.

## Installation

Add `qring` to your `Cargo.toml`:

```toml
[dependencies]
qring = "0.1"
```

To use it on a target without `std`, disable default features (none are enabled by default):

```toml
[dependencies]
qring = { version = "0.1", default-features = false }
```

## Quick Start

```rust
use qring::spsc::SpscQueue;

// Capacity is a power-of-two const generic; usable capacity is N - 1.
let mut queue = SpscQueue::<u32, 16>::new();
let (mut producer, mut consumer) = queue.split();

producer.push(42).unwrap();
assert_eq!(consumer.pop(), Some(42));
```

### Producer and consumer on separate threads

```rust
use qring::spsc::SpscQueue;
use std::thread;

let mut queue = SpscQueue::<u64, 1024>::new();
let (mut producer, mut consumer) = queue.split();

let producer = thread::spawn(move || {
    for i in 0..10_000u64 {
        // `push` returns the value back in `Err` when the queue is full.
        while producer.push(i).is_err() {
            std::hint::spin_loop();
        }
    }
});

let consumer = thread::spawn(move || {
    let mut sum = 0u64;
    for _ in 0..10_000 {
        loop {
            if let Some(v) = consumer.pop() {
                sum += v;
                break;
            }
            std::hint::spin_loop();
        }
    }
    sum
});

producer.join().unwrap();
assert_eq!(consumer.join().unwrap(), 49_995_000);
```

### Bulk transfers

```rust
use qring::spsc::SpscQueue;

let mut queue = SpscQueue::<u8, 8>::new();
let (mut producer, mut consumer) = queue.split();

// Push as many as fit, reporting how many were written.
let written = producer.push_slice(&[1, 2, 3, 4]);
assert_eq!(written, 4);

// All-or-nothing: rejected whole if it does not fit.
assert_eq!(producer.push_exact(&[5, 6, 7]), Ok(()));

let mut out = [0u8; 5];
assert_eq!(consumer.pop_slice(&mut out), 5);
assert_eq!(out, [1, 2, 3, 4, 5]);

// All-or-nothing read.
let mut small = [0u8; 2];
assert_eq!(consumer.pop_exact(&mut small), Err(()));
```

## API Overview

### `SpscQueue<T, N>`

| Method | Description |
| --- | --- |
| `new()` | `const fn`; creates an empty queue. `N` must be a power of two. |
| `split(&mut self)` | Borrows the queue and returns `(Producer, Consumer)`. |
| `capacity(&self)` | Returns the usable capacity, `N - 1`. |

### `Producer<'a, T, N>`

| Method | Description |
| --- | --- |
| `push(val)` | Push one value; returns `Err(val)` when full. |
| `push_slice(&[T])` | Push a prefix that fits; returns the number written. |
| `push_exact(&[T])` | Push the whole slice or nothing. |
| `vacant_len()` | Snapshot of free slots. |
| `is_full()` / `is_empty()` | Snapshot predicates. |
| `capacity()` | Returns `N - 1`. |

### `Consumer<'a, T, N>`

| Method | Description |
| --- | --- |
| `pop()` | Pop one value, or `None` when empty. |
| `pop_slice(&mut [T])` | Pop up to `slice.len()` values; returns the number read. |
| `pop_exact(&mut [T])` | Pop exactly `slice.len()` or nothing. |
| `occupied_len()` | Snapshot of buffered elements. |
| `is_empty()` / `is_full()` | Snapshot predicates. |
| `capacity()` | Returns `N - 1`. |

The `*_len` and `is_*` methods observe a concurrent queue and are snapshots; they can be stale the moment they return and should be treated as estimates.

## Design Notes

- **Ring buffer with one reserved slot.** A full queue and an empty queue would otherwise be indistinguishable, so one slot is always left vacant. The usable capacity is therefore `N - 1`.
- **Power-of-two sizing.** Index wraparound uses `& (N - 1)` instead of a modulo, and `N.is_power_of_two()` is asserted at compile time inside `new` via a const check.
- **Cache-line padding.** `head` and `tail` each live in a `#[repr(align(64))]` wrapper so the producer's writes to `head` and the consumer's writes to `tail` never share a cache line, avoiding false sharing.
- **Memory ordering.** The owning side loads its own index with `Relaxed`, acquires the peer index with `Acquire`, writes the slot, then publishes with `Release`.
- **Ownership.** `split` borrows the queue mutably, so the compiler guarantees a single `Producer` and a single `Consumer`. Both are `Send` when `T: Send` and may live on different threads.
- **No `T: Copy` for single ops.** `push`/`pop` work with any `T`; only the bulk slice operations require `T: Copy` because they use `copy_nonoverlapping`.

## Testing and Benchmarks

Run the unit and integration tests:

```bash
cargo test
```

The integration test `tests/stress_test.rs` compares `qring` against `heapless::spsc` and `ringbuf::StaticRb` on a 100M-operation, 64-byte-payload workload with burst batching:

```bash
cargo test --release cmp_spsc_crates -- --nocapture
```

For unsafety validation, the test suite is designed to run cleanly under Miri:

```bash
cargo +nightly miri test
```

## Minimum Supported Rust Version

Rust 1.85 (edition 2024).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this project by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
