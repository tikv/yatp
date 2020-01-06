// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

const NANOSECONDS_PER_SECOND: u32 = 1_000_000_000;

#[derive(Copy, Clone, Debug)]
pub struct CoarseInstant {
    sec: u64,
    nsec: u32,
}

impl CoarseInstant {
    #[cfg(not(target_os = "linux"))]
    pub fn now() -> CoarseInstant {
        let n = std::time::SystemTime::now();
        let dur = n.duration_since(std::time::SystemTime::UNIX_EPOCH);

        CoarseInstant {
            sec: dur.as_secs() as u64,
            nsec: dur.subsec_nanos(),
        }
    }

    #[cfg(target_os = "linux")]
    pub fn now() -> CoarseInstant {
        let mut t = std::mem::MaybeUninit::uninit();
        let errno = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_COARSE, t.as_mut_ptr()) };
        if errno == 0 {
            let t = unsafe { t.assume_init() };
            CoarseInstant {
                sec: t.tv_sec as u64,
                nsec: t.tv_nsec as u32,
            }
        } else {
            panic!("unable to get time, error code: {}", errno);
        }
    }

    pub fn elapsed(&self) -> Duration {
        let n = CoarseInstant::now();
        n.duration_since(*self)
    }

    pub fn duration_since(&self, i: CoarseInstant) -> Duration {
        if self.sec > i.sec {
            if self.nsec >= i.nsec {
                Duration::new(self.sec - i.sec, self.nsec - i.nsec)
            } else {
                Duration::new(
                    self.sec - i.sec - 1,
                    NANOSECONDS_PER_SECOND - (i.nsec - self.nsec),
                )
            }
        } else if self.sec == i.sec && self.nsec >= i.nsec {
            Duration::new(0, self.nsec - i.nsec)
        } else {
            Duration::new(0, 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_coarse_instant_on_smp() {
        let zero = Duration::from_millis(0);
        let timer = CoarseInstant::now();
        for i in 0..1_000_000 {
            let now = CoarseInstant::now();
            if i % 100 == 0 {
                thread::yield_now();
            }
            assert!(now.elapsed() >= zero);
        }
        std::thread::sleep(Duration::from_millis(10));
        assert!(timer.elapsed() > zero);
        assert!(CoarseInstant::now().duration_since(timer) > zero);
    }
}
