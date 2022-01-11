use std::{num::NonZeroUsize, mem::replace};

pub(super) struct Rng {
    xorshift: NonZeroUsize,
}

impl Rng {
    pub(super) fn new(seed: usize) -> Self {
        Self {
            xorshift: NonZeroUsize::new(seed)
                .or(NonZeroUsize::new(0xdeadbeef))
                .unwrap()
        }
    }
}

impl Iterator for Rng {
    type Item = NonZeroUsize;

    fn next(&mut self) -> Option<Self::Item> {
        let shifts = match usize::BITS {
            64 => (13, 7, 17),
            32 => (13, 17, 5),
            _ => unreachable!(),
        };

        let mut xs = xorshift.get();
        xs ^= xs >> shifts.0;
        xs ^= xs << shifts.1;
        xs ^= xs >> shifts.2;

        self.xorshift = NonZeroUsize::new(xs).unwrap();
        Some(self.xorshift)
    }
}

pub(super) RandomSequence {
    range: NonZeroUsize,
    co_prime: NonZeroUsize,
}

impl RandomSequence {
    pub(super) fn new(range: NonZeroUsize) -> Self {
        fn gcd(mut a: usize, mut b: usize) -> usize {
            while a != b {
                if a > b {
                    a -= b;
                } else {
                    b -= a;
                }
            }
            a
        }

        Self {
            range,
            co_prime: (range.get() / 2 .. range.get())
                .filter(|&n| gcd(n, range.get()) == 1)
                .next()
                .or(NonZeroUsize::new(1))
                .unwrap(),
        }
    }

    pub fn iter(&self, seed: usize) -> impl Iterator<Item = usize> {
        let prime = self.co_prime.get();
        let range = self.range.get();
        let mut index = seed % range;

        (0..range).map(move |_| {
            let mut new_index = index + prime;
            if new_index >= range {
                new_index -= range;
            }

            assert!(new_index < range);
            replace(&mut index, new_index)
        })
    }
}