#![no_std]

pub mod spsc;

#[cfg(test)]
mod tests {
    use crate::spsc::SpscQueue;

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
}
