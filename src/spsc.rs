//! A lock-free single-producer single-consumer (SPSC) queue.
//!
//! The queue has a fixed capacity determined at compile time by the const
//! generic `N`. It is backed by a ring buffer where one slot is always kept
//! free to distinguish a full queue from an empty one, so the usable capacity
//! is `N - 1`.
//!
//! Because it is an SPSC queue, exactly one thread may hold the [`Producer`]
//! and exactly one thread may hold the [`Consumer`]. Ownership is enforced by
//! [`SpscQueue::split`], which borrows the queue and hands out one of each.
//!
//! # Example
//!
//! ```
//! use axon::spsc::SpscQueue;
//!
//! let mut queue = SpscQueue::<u32, 16>::new();
//! let (mut producer, mut consumer) = queue.split();
//!
//! producer.push(42).unwrap();
//! assert_eq!(consumer.pop(), Some(42));
//! ```

use core::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicUsize, Ordering},
};

#[repr(align(64))]
struct CacheAligned<T>(T);

impl<T> Deref for CacheAligned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for CacheAligned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// A fixed-capacity, lock-free single-producer single-consumer queue.
///
/// The queue stores up to `N - 1` elements of type `T`. `N` **must** be a power
/// of two; this is checked at compile time. The extra slot is used to tell a
/// full queue apart from an empty one.
///
/// Create a queue with [`SpscQueue::new`] and then call
/// [`split`](SpscQueue::split) to obtain a [`Producer`] and a [`Consumer`].
/// Each half may be moved to a different thread, but there may only ever be one
/// of each.
pub struct SpscQueue<T, const N: usize> {
    buffer: [UnsafeCell<MaybeUninit<T>>; N],

    // Aligning head and tail to 64-byte cache line boundaries eliminates
    // false sharing (cache line bouncing) between producer and consumer cores. so
    // code execution speed is at maximum
    head: CacheAligned<AtomicUsize>,
    tail: CacheAligned<AtomicUsize>,
}

unsafe impl<T: Send, const N: usize> Sync for SpscQueue<T, N> {}
unsafe impl<T: Send, const N: usize> Send for SpscQueue<T, N> {}

impl<T, const N: usize> Drop for SpscQueue<T, N> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

impl<T, const N: usize> SpscQueue<T, N> {
    const INIT: UnsafeCell<MaybeUninit<T>> = UnsafeCell::new(MaybeUninit::uninit());

    const CHECK_POWER_OF_TWO: () = {
        assert!(
            N.is_power_of_two(),
            "SpscQueue capacity N must be a power of two!"
        );
    };

    /// Creates a new empty queue.
    ///
    /// `N` is the size of the backing ring buffer and must be a power of two;
    /// passing a non-power-of-two value fails compilation. The usable capacity
    /// is `N - 1`, available via [`capacity`](SpscQueue::capacity).
    ///
    /// This is a `const fn`, so it can be used in `static`/`const` contexts.
    ///
    /// # Example
    ///
    /// ```
    /// use axon::spsc::SpscQueue;
    ///
    /// let queue = SpscQueue::<u8, 8>::new();
    /// assert_eq!(queue.capacity(), 7);
    /// ```
    pub const fn new() -> Self {
        // Touch the const assertion during construction
        // to guarantee a build failure if invalid
        let _ = Self::CHECK_POWER_OF_TWO;

        SpscQueue {
            buffer: [Self::INIT; N],
            head: CacheAligned(AtomicUsize::new(0)),
            tail: CacheAligned(AtomicUsize::new(0)),
        }
    }

    fn push(&self, val: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        // replacement for modulus operator
        let next_head = (head + 1) & (N - 1);

        if next_head == tail {
            return Err(val);
        }

        unsafe {
            (*self.buffer[head].get()).write(val);
        }

        self.head.store(next_head, Ordering::Release);

        Ok(())
    }

    fn pop(&self) -> Option<T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        if tail == head {
            return None;
        }

        let val = unsafe { (*self.buffer[tail].get()).assume_init_read() };

        // replacement for modulus operator
        let next_tail = (tail + 1) & (N - 1);
        self.tail.store(next_tail, Ordering::Release);

        Some(val)
    }

    /// Splits the queue into a [`Producer`] and a [`Consumer`].
    ///
    /// The returned halves borrow the queue mutably for the duration of their
    /// lifetime, so no other access to the queue is possible while they exist.
    /// Move the [`Producer`] to the producing thread and the [`Consumer`] to
    /// the consuming thread.
    ///
    /// # Example
    ///
    /// ```
    /// use axon::spsc::SpscQueue;
    ///
    /// let mut queue = SpscQueue::<u32, 4>::new();
    /// let (mut producer, mut consumer) = queue.split();
    ///
    /// producer.push(1).unwrap();
    /// assert_eq!(consumer.pop(), Some(1));
    /// ```
    pub fn split(&mut self) -> (Producer<'_, T, N>, Consumer<'_, T, N>) {
        (Producer { queue: self }, Consumer { queue: self })
    }

    /// Returns the number of elements the queue can hold.
    ///
    /// This is always `N - 1` because one slot is reserved internally to
    /// distinguish a full queue from an empty one.
    pub const fn capacity(&self) -> usize {
        N - 1
    }
}

/// The producing half of an [`SpscQueue`], obtained from [`SpscQueue::split`].
///
/// This handle owns the sole permission to enqueue values. It may be sent to
/// another thread, but there must only ever be one [`Producer`] per queue.
pub struct Producer<'a, T, const N: usize> {
    queue: &'a SpscQueue<T, N>,
}

impl<'a, T, const N: usize> Producer<'a, T, N> {
    /// Pushes a single value onto the queue.
    ///
    /// On success returns `Ok(())`. If the queue is full the value is returned
    /// back inside `Err` so no data is lost.
    ///
    /// # Example
    ///
    /// ```
    /// use axon::spsc::SpscQueue;
    ///
    /// let mut queue = SpscQueue::<u32, 4>::new();
    /// let (mut producer, _consumer) = queue.split();
    ///
    /// assert_eq!(producer.push(7), Ok(()));
    /// ```
    #[inline(always)]
    pub fn push(&mut self, val: T) -> Result<(), T> {
        self.queue.push(val)
    }

    /// Pushes as many items from `slice` as fit, returning how many were written.
    ///
    /// The first `written` elements of `slice` are copied into the queue. If the
    /// queue has less free space than `slice` is long, the remainder is not
    /// written and the returned count is smaller than `slice.len()`. A return
    /// value of `0` means the queue was full.
    ///
    /// This is more efficient than calling [`push`](Producer::push) in a loop
    /// because the items are copied in bulk.
    ///
    /// # Example
    ///
    /// ```
    /// use axon::spsc::SpscQueue;
    ///
    /// let mut queue = SpscQueue::<u8, 8>::new();
    /// let (mut producer, _consumer) = queue.split();
    ///
    /// let written = producer.push_slice(&[1, 2, 3, 4]);
    /// assert_eq!(written, 4);
    /// ```
    pub fn push_slice(&mut self, slice: &[T]) -> usize
    where
        T: Copy, // item must implement copy
    {
        let head = self.queue.head.load(Ordering::Relaxed);
        let tail = self.queue.tail.load(Ordering::Acquire);

        let avail = (tail.wrapping_sub(head).wrapping_sub(1)) & (N - 1);
        let to_write = slice.len().min(avail);
        if to_write == 0 {
            return 0;
        }

        let first_chunk = (N - head).min(to_write);
        let second_chunk = to_write - first_chunk;

        unsafe {
            let src_ptr = slice.as_ptr();
            let buf_ptr = self.queue.buffer.as_ptr() as *mut UnsafeCell<MaybeUninit<T>> as *mut T;

            // memcpy the first chunk
            core::ptr::copy_nonoverlapping(src_ptr, buf_ptr.add(head), first_chunk);

            // memcpy if second chunk is present due to wrap around
            if second_chunk > 0 {
                core::ptr::copy_nonoverlapping(src_ptr.add(first_chunk), buf_ptr, second_chunk);
            }
        }

        // bit mask replacement for modulus
        let next_head = (head + to_write) & (N - 1);
        self.queue.head.store(next_head, Ordering::Release);
        to_write
    }

    /// Pushes every element of `slice`, or nothing at all.
    ///
    /// If the queue has at least `slice.len()` vacant slots, the whole slice is
    /// copied in and `Ok(())` is returned. Otherwise the queue is left
    /// untouched and `Err(())` is returned.
    ///
    /// Unlike [`push_slice`](Producer::push_slice), this is all-or-nothing: it
    /// never writes a partial prefix of `slice`.
    ///
    /// # Example
    ///
    /// ```
    /// use axon::spsc::SpscQueue;
    ///
    /// let mut queue = SpscQueue::<u8, 8>::new();
    /// let (mut producer, mut consumer) = queue.split();
    ///
    /// assert_eq!(producer.push_exact(&[1, 2, 3]), Ok(()));
    /// assert_eq!(consumer.occupied_len(), 3);
    ///
    /// // Only 4 slots remain, so this larger slice is rejected whole.
    /// assert_eq!(producer.push_exact(&[4, 5, 6, 7, 8]), Err(()));
    /// ```
    pub fn push_exact(&mut self, slice: &[T]) -> Result<(), ()>
    where
        T: Copy,
    {
        if self.vacant_len() < slice.len() {
            return Err(());
        }

        self.push_slice(slice);
        Ok(())
    }

    /// Returns the number of elements that can still be pushed before the queue
    /// becomes full.
    ///
    /// This is a snapshot of a concurrent queue: another thread may be
    /// consuming items at the same time, so the value can be stale as soon as
    /// it is read. Treat it as an estimate.
    pub fn vacant_len(&self) -> usize {
        let head = self.queue.head.load(Ordering::Relaxed);
        let tail = self.queue.tail.load(Ordering::Acquire);

        tail.wrapping_sub(head).wrapping_sub(1) & (N - 1)
    }

    /// Returns the maximum capacity of the queue (`N - 1`).
    #[inline(always)]
    pub const fn capacity(&self) -> usize {
        N - 1
    }

    /// Returns `true` if the queue currently has no vacant slots.
    ///
    /// Because the queue is concurrent this is only a snapshot; see
    /// [`vacant_len`](Producer::vacant_len).
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.vacant_len() == 0
    }

    /// Returns `true` if the queue currently contains no elements.
    ///
    /// Because the queue is concurrent this is only a snapshot; see
    /// [`vacant_len`](Producer::vacant_len).
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.vacant_len() == (N - 1)
    }
}

/// The consuming half of an [`SpscQueue`], obtained from [`SpscQueue::split`].
///
/// This handle owns the sole permission to dequeue values. It may be sent to
/// another thread, but there must only ever be one [`Consumer`] per queue.
pub struct Consumer<'a, T, const N: usize> {
    queue: &'a SpscQueue<T, N>,
}

impl<'a, T, const N: usize> Consumer<'a, T, N> {
    /// Pops a single value from the queue.
    ///
    /// Returns `Some(value)` if an element was available, or `None` if the
    /// queue was empty.
    ///
    /// # Example
    ///
    /// ```
    /// use axon::spsc::SpscQueue;
    ///
    /// let mut queue = SpscQueue::<u32, 4>::new();
    /// let (mut producer, mut consumer) = queue.split();
    ///
    /// assert_eq!(consumer.pop(), None);
    /// producer.push(7).unwrap();
    /// assert_eq!(consumer.pop(), Some(7));
    /// ```
    #[inline(always)]
    pub fn pop(&mut self) -> Option<T> {
        self.queue.pop()
    }

    /// Pops items into `slice`, returning how many were read.
    ///
    /// Up to `slice.len()` elements are moved out of the queue and into the
    /// front of `slice`. The returned count is the number actually read; it can
    /// be less than `slice.len()` if the queue held fewer items, and `0` if the
    /// queue was empty.
    ///
    /// This is more efficient than calling [`pop`](Consumer::pop) in a loop
    /// because the items are copied in bulk.
    ///
    /// # Example
    ///
    /// ```
    /// use axon::spsc::SpscQueue;
    ///
    /// let mut queue = SpscQueue::<u8, 8>::new();
    /// let (mut producer, mut consumer) = queue.split();
    ///
    /// producer.push_slice(&[1, 2, 3, 4]);
    ///
    /// let mut out = [0u8; 2];
    /// assert_eq!(consumer.pop_slice(&mut out), 2);
    /// assert_eq!(out, [1, 2]);
    /// ```
    pub fn pop_slice(&mut self, slice: &mut [T]) -> usize
    where
        T: Copy,
    {
        let tail = self.queue.tail.load(Ordering::Relaxed);
        let head = self.queue.head.load(Ordering::Acquire);

        let avail = head.wrapping_sub(tail) & (N - 1);
        let to_read = slice.len().min(avail);
        if to_read == 0 {
            return 0;
        }

        let first_chunk = (N - tail).min(to_read);
        let second_chunk = to_read - first_chunk;

        unsafe {
            let dst_ptr = slice.as_mut_ptr();
            let buf_ptr =
                self.queue.buffer.as_ptr() as *const UnsafeCell<MaybeUninit<T>> as *const T;

            // memcpy the first chunk
            core::ptr::copy_nonoverlapping(buf_ptr.add(tail), dst_ptr, first_chunk);

            // memcpy the second chunk if present due to wraparound
            if second_chunk > 0 {
                core::ptr::copy_nonoverlapping(buf_ptr, dst_ptr.add(first_chunk), second_chunk);
            }
        }

        // bit ops for modulus replacement
        let next_tail = (tail + to_read) & (N - 1);
        self.queue.tail.store(next_tail, Ordering::Release);
        to_read
    }


    /// Pops exactly `slice.len()` elements into `slice`, or nothing at all.
    ///
    /// If the queue holds at least `slice.len()` elements, the whole `slice` is
    /// filled and `Ok(())` is returned. Otherwise the queue and `slice` are left
    /// untouched and `Err(())` is returned.
    ///
    /// Unlike [`pop_slice`](Consumer::pop_slice), this is all-or-nothing: it
    /// never reads a partial prefix into `slice`.
    ///
    /// # Example
    ///
    /// ```
    /// use axon::spsc::SpscQueue;
    ///
    /// let mut queue = SpscQueue::<u8, 8>::new();
    /// let (mut producer, mut consumer) = queue.split();
    ///
    /// producer.push_slice(&[1, 2, 3, 4]);
    ///
    /// let mut out = [0u8; 3];
    /// assert_eq!(consumer.pop_exact(&mut out), Ok(()));
    /// assert_eq!(out, [1, 2, 3]);
    ///
    /// // Only one element remains, so this larger slice is rejected whole.
    /// let mut too_big = [0u8; 4];
    /// assert_eq!(consumer.pop_exact(&mut too_big), Err(()));
    /// ```
    pub fn pop_exact(&mut self, slice: &mut [T]) -> Result<(), ()>
    where
        T: Copy,
    {
        if self.occupied_len() < slice.len() {
            return Err(());
        }

        self.pop_slice(slice);
        Ok(())
    }

    /// Returns the number of elements currently held in the queue and available
    /// to be popped.
    ///
    /// This is a snapshot of a concurrent queue: the producer may be pushing
    /// items at the same time, so the value can be stale as soon as it is read.
    /// Treat it as an estimate.
    #[inline(always)]
    pub fn occupied_len(&self) -> usize {
        let tail = self.queue.tail.load(Ordering::Relaxed);
        let head = self.queue.head.load(Ordering::Acquire);
        head.wrapping_sub(tail) & (N - 1)
    }

    /// Returns the maximum capacity of the queue (`N - 1`).
    #[inline(always)]
    pub const fn capacity(&self) -> usize {
        N - 1
    }

    /// Returns `true` if the queue currently contains no elements.
    ///
    /// Because the queue is concurrent this is only a snapshot; see
    /// [`occupied_len`](Consumer::occupied_len).
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.occupied_len() == 0
    }

    /// Returns `true` if the queue currently has no vacant slots.
    ///
    /// Because the queue is concurrent this is only a snapshot; see
    /// [`occupied_len`](Consumer::occupied_len).
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.occupied_len() == (N - 1)
    }
}
