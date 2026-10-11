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

pub struct SpscQueue<T, const N: usize> {
    buffer: [UnsafeCell<MaybeUninit<T>>; N],

    // aligning head and tail to 64 bytes so that one L1 cache fetch fetches only 
    // head or tail, so no false fetching happens for other data, thus maintaining
    // code execution speed at maximum
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

    pub fn split(&mut self) -> (Producer<'_, T, N>, Consumer<'_, T, N>) {
        (Producer { queue: self }, Consumer { queue: self })
    }

    pub const fn capacity(&self) -> usize {
        N - 1
    }
}

pub struct Producer<'a, T, const N: usize> {
    queue: &'a SpscQueue<T, N>,
}

impl<'a, T, const N: usize> Producer<'a, T, N> {
    #[inline(always)]
    pub fn push(&mut self, val: T) -> Result<(), T> {
        self.queue.push(val)
    }

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

    pub fn vacant_len(&self) -> usize {
        let head = self.queue.head.load(Ordering::Relaxed);
        let tail = self.queue.tail.load(Ordering::Acquire);

        tail.wrapping_sub(head).wrapping_sub(1) & (N - 1)
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.vacant_len() == 0
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.vacant_len() == (N - 1)
    }
}

pub struct Consumer<'a, T, const N: usize> {
    queue: &'a SpscQueue<T, N>,
}

impl<'a, T, const N: usize> Consumer<'a, T, N> {
    #[inline(always)]
    pub fn pop(&mut self) -> Option<T> {
        self.queue.pop()
    }

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

    #[inline(always)]
    pub fn occupied_len(&self) -> usize {
        let tail = self.queue.tail.load(Ordering::Relaxed);
        let head = self.queue.head.load(Ordering::Acquire);
        head.wrapping_sub(tail) & (N - 1)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.occupied_len() == 0
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.occupied_len() == (N - 1)
    }
}
