// tests/stress_test.rs

use std::hint::black_box;
use std::thread;
use std::time::Instant;

use axon::spsc::SpscQueue;
use heapless::spsc::Queue as HeaplessQueue;
use ringbuf::{StaticRb, traits::*};

const QUEUE_SIZE: usize = 1024;
const WARMUP_ITEMS: usize = 1_000_000;
const TOTAL_ITEMS: usize = 100_000_000;
const BATCH_SIZE: usize = 16; // Simulates driver burst batching

/// 64-byte payload representing an IPC message or network frame header
#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(C)]
struct Message {
    id: usize,
    checksum: u64,
    payload: [u8; 48],
}

impl Message {
    #[inline(always)]
    fn new(id: usize) -> Self {
        Self {
            id,
            checksum: (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
            payload: [id as u8; 48],
        }
    }

    /// Simulates consumer packet handling / checksum validation
    #[inline(always)]
    fn process(&self) -> u64 {
        black_box(self.checksum ^ (self.id as u64))
    }
}

macro_rules! run_real_world_bench {
    ($name:expr, $setup:expr, $push_mth:ident, $pop_mth:ident) => {{
        let (mut prod, mut cons) = $setup;

        // 1. WARMUP PHASE
        let prod_warmup = thread::spawn(move || {
            for i in 0..WARMUP_ITEMS {
                let msg = Message::new(i);
                while prod.$push_mth(msg).is_err() {
                    std::hint::spin_loop();
                }
            }
            prod
        });

        let cons_warmup = thread::spawn(move || {
            let mut expected = 0;
            while expected < WARMUP_ITEMS {
                if let Some(msg) = cons.$pop_mth() {
                    black_box(msg.process());
                    expected += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
            cons
        });

        let mut prod = prod_warmup.join().unwrap();
        let mut cons = cons_warmup.join().unwrap();

        // 2. TIMED REAL-WORLD BENCHMARK (Batching + Consumer Processing)
        let start = Instant::now();

        let prod_handle = thread::spawn(move || {
            let mut sent = 0;
            while sent < TOTAL_ITEMS {
                // Generate and push a burst of BATCH_SIZE items
                for b in 0..BATCH_SIZE {
                    let msg = Message::new(sent + b);
                    while prod.$push_mth(msg).is_err() {
                        std::hint::spin_loop();
                    }
                }
                sent += BATCH_SIZE;
                // Yield CPU brief fraction to simulate interrupt/driver batch intervals
                black_box(sent);
            }
        });

        let cons_handle = thread::spawn(move || {
            let mut received = 0;
            while received < TOTAL_ITEMS {
                if let Some(msg) = cons.$pop_mth() {
                    // Execute simulated workload per item
                    black_box(msg.process());
                    received += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        prod_handle.join().unwrap();
        cons_handle.join().unwrap();

        let elapsed = start.elapsed();
        let mops = (TOTAL_ITEMS as f64 / 1_000_000.0) / elapsed.as_secs_f64();
        println!("{:<25} | {:>6.2} Mops/sec | {:.3?}", $name, mops, elapsed);
    }};
}

#[test]
fn cmp_spsc_crates() {
    println!("\n=======================================================");
    println!("     REAL-WORLD SPSC BENCHMARK (Batching & Workload)    ");
    println!("=======================================================");
    println!(
        "Payload: 64-byte Message | Batches: {} | Total: 100M Ops",
        BATCH_SIZE
    );
    println!("-------------------------------------------------------");

    run_real_world_bench!(
        "Axon SpscQueue",
        {
            let q = Box::leak(Box::new(SpscQueue::<Message, QUEUE_SIZE>::new()));
            q.split()
        },
        push,
        pop
    );

    run_real_world_bench!(
        "heapless::spsc",
        {
            let q = Box::leak(Box::new(HeaplessQueue::<Message, QUEUE_SIZE>::new()));
            q.split()
        },
        enqueue,
        dequeue
    );

    run_real_world_bench!(
        "ringbuf::StaticRb",
        {
            let q = Box::leak(Box::new(StaticRb::<Message, QUEUE_SIZE>::default()));
            q.split_ref()
        },
        try_push,
        try_pop
    );

    println!("=======================================================\n");
}
