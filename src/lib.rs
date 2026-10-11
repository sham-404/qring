#![no_std]

pub mod spsc;

#[cfg(test)]
mod tests {
    use super::spsc::SpscQueue;
    use core::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn pushpop() {
        let mut queue = SpscQueue::<i32, 16>::new();
        let (mut prod, mut cons) = queue.split();

        assert_eq!(cons.pop(), None);

        assert!(prod.push(10).is_ok());
        assert!(prod.push(20).is_ok());
        assert!(prod.push(30).is_ok());

        assert_eq!(cons.pop(), Some(10));
        assert_eq!(cons.pop(), Some(20));
        assert_eq!(cons.pop(), Some(30));
        assert_eq!(cons.pop(), None);

        for i in 0..prod.vacant_len() as i32 {
            assert_eq!(prod.push(i), Ok(()));
        }

        assert_eq!(prod.push(999), Err(999));

        for i in 0..cons.occupied_len() as i32 {
            assert_eq!(cons.pop(), Some(i));
        }

        assert_eq!(cons.pop(), None);

        assert!(prod.push(100).is_ok());
        assert_eq!(cons.pop(), Some(100));
        assert_eq!(cons.pop(), None);
    }

    /// Tests split-chunk `copy_nonoverlapping` wraparound logic across the buffer boundary.
    #[test]
    fn miri_wraparound_bulk_ops() {
        let mut queue = SpscQueue::<u8, 8>::new();
        let (mut prod, mut cons) = queue.split();

        assert_eq!(prod.push_slice(&[1, 2, 3, 4, 5]), 5);

        let mut tmp = [0u8; 4];
        assert_eq!(cons.pop_slice(&mut tmp), 4);
        assert_eq!(tmp, [1, 2, 3, 4]);

        assert_eq!(prod.push_exact(&[10, 20, 30, 40, 50]), Ok(()));

        let mut out = [0u8; 6];
        assert_eq!(cons.pop_slice(&mut out), 6);
        assert_eq!(out, [5, 10, 20, 30, 40, 50]);
    }

    #[test]
    fn miri_debug_formatting() {
        struct NonDebug; // Type does not implement Debug

        let mut queue = SpscQueue::<NonDebug, 8>::new();
        let _ = core::format_args!("{:?}", queue);

        let (producer, consumer) = queue.split();
        let _ = core::format_args!("{:?}", producer);
        let _ = core::format_args!("{:?}", consumer);
    }

    /// Verifies Drop for SpscQueue correctly cleans up unconsumed items without leaks or double-frees using core only.
    #[test]
    fn miri_drop_unconsumed_leak_check() {
        struct DropTracker(*const AtomicUsize);

        impl Drop for DropTracker {
            fn drop(&mut self) {
                unsafe {
                    (*self.0).fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let drops = AtomicUsize::new(0);

        {
            let mut queue = SpscQueue::<DropTracker, 8>::new();
            let (mut prod, mut cons) = queue.split();

            let _ = prod.push(DropTracker(&drops));
            let _ = prod.push(DropTracker(&drops));
            let _ = prod.push(DropTracker(&drops));

            drop(cons.pop());
            assert_eq!(drops.load(Ordering::Relaxed), 1);
        } // queue drops here, executing Drop for SpscQueue and cleaning up the remaining 2 items

        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }
}
