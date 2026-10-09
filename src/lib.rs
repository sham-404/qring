#![no_std]

pub mod spsc;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use crate::spsc::SpscQueue;

    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    #[test]
    fn pushpop() {
        let queue = SpscQueue::<i32, 10>::new();

        assert_eq!(queue.pop(), None);

        assert!(queue.push(10).is_ok());
        assert!(queue.push(20).is_ok());
        assert!(queue.push(30).is_ok());

        assert_eq!(queue.pop(), Some(10));
        assert_eq!(queue.pop(), Some(20));
        assert_eq!(queue.pop(), Some(30));
        assert_eq!(queue.pop(), None);

        for i in 0..9 {
            assert_eq!(queue.push(i), Ok(()));
        }

        assert_eq!(queue.push(999), Err(999));

        for i in 0..9 {
            assert_eq!(queue.pop(), Some(i));
        }

        assert_eq!(queue.pop(), None);

        assert!(queue.push(100).is_ok());
        assert_eq!(queue.pop(), Some(100));
        assert_eq!(queue.pop(), None);
    }
}
