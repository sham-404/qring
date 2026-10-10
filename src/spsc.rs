use core::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};

pub struct SpscQueue<T, const N: usize> {
    buffer: [UnsafeCell<MaybeUninit<T>>; N],
    head: AtomicUsize,
    tail: AtomicUsize,
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
    pub const fn new() -> Self {
        SpscQueue {
            buffer: [Self::INIT; N],
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    fn push(&self, val: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);

        let tail = self.tail.load(Ordering::Acquire);
        let next_head = (head + 1) % N;

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

        let next_tail = (tail + 1) % N;
        self.tail.store(next_tail, Ordering::Release);

        Some(val)
    }

    pub fn split(&mut self) -> (Producer<'_, T, N>, Consumer<'_, T, N>) {
        (Producer { queue: self }, Consumer { queue: self })
    }
}

pub struct Producer<'a, T, const N: usize> {
    queue: &'a SpscQueue<T, N>,
}

impl<'a, T, const N: usize> Producer<'a, T, N> {
    pub fn push(&mut self, val: T) -> Result<(), T> {
        self.queue.push(val)
    }
}

pub struct Consumer<'a, T, const N: usize> {
    queue: &'a SpscQueue<T, N>,
}

impl<'a, T, const N: usize> Consumer<'a, T, N> {
    pub fn pop(&mut self) -> Option<T> {
        self.queue.pop()
    }
}
