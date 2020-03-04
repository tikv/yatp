// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A single level work stealing task queue.
//!
//! The instant when the task cell is pushed into the queue is recorded
//! in the extras.

use super::{Pop, TaskCell};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use rand::prelude::*;
use std::iter;
use std::sync::Arc;
use std::time::Instant;

/// The injector of a single level work stealing task queue.
pub struct TaskInjector<T>(Arc<Injector<T>>);

impl<T: TaskCell> Clone for TaskInjector<T> {
    fn clone(&self) -> Self {
        TaskInjector(self.0.clone())
    }
}

fn set_schedule_time<T>(task_cell: &mut T)
where
    T: TaskCell,
{
    task_cell.mut_extras().schedule_time = Some(Instant::now());
}

impl<T> TaskInjector<T>
where
    T: TaskCell + Send,
{
    /// Pushes the task cell to the queue. The schedule time in the extras is
    /// assigned to be now.
    pub fn push(&self, mut task_cell: T) {
        set_schedule_time(&mut task_cell);
        self.0.push(task_cell);
    }
}

/// The local queue of a single level work stealing task queue.
pub struct LocalQueue<T> {
    local_queue: Worker<T>,
    injector: Arc<Injector<T>>,
    stealers: Vec<Stealer<T>>,
}

impl<T> LocalQueue<T>
where
    T: TaskCell,
{
    pub fn push(&mut self, mut task_cell: T) {
        set_schedule_time(&mut task_cell);
        self.local_queue.push(task_cell);
    }

    pub fn pop(&mut self) -> Option<Pop<T>> {
        fn into_pop<T>(mut t: T, from_local: bool) -> Pop<T>
        where
            T: TaskCell,
        {
            let schedule_time = t.mut_extras().schedule_time.unwrap();
            Pop {
                task_cell: t,
                schedule_time,
                from_local,
            }
        }

        if let Some(t) = self.local_queue.pop() {
            return Some(into_pop(t, true));
        }
        let mut need_retry = true;
        while need_retry {
            need_retry = false;
            match self.injector.steal_batch_and_pop(&self.local_queue) {
                Steal::Success(t) => return Some(into_pop(t, false)),
                Steal::Retry => need_retry = true,
                _ => {}
            }
            if !self.stealers.is_empty() {
                let mut found = None;
                for (idx, stealer) in self.stealers.iter().enumerate() {
                    match stealer.steal_batch_and_pop(&self.local_queue) {
                        Steal::Success(t) => {
                            found = Some((idx, into_pop(t, false)));
                            break;
                        }
                        Steal::Retry => need_retry = true,
                        _ => {}
                    }
                }
                if let Some((idx, task)) = found {
                    let last_pos = self.stealers.len() - 1;
                    self.stealers.swap(idx, last_pos);
                    return Some(task);
                }
            }
        }
        None
    }

    pub fn has_tasks_or_pull(&mut self) -> bool {
        if !self.local_queue.is_empty() {
            return true;
        }
        loop {
            match self.injector.steal_batch(&self.local_queue) {
                Steal::Success(()) => return true,
                Steal::Empty => return false,
                Steal::Retry => {}
            }
        }
    }
}

/// Creates a single level work stealing task queue with `local_num` local queues.
pub fn create<T>(local_num: usize) -> (TaskInjector<T>, Vec<LocalQueue<T>>) {
    let injector = Arc::new(Injector::new());
    let workers: Vec<_> = iter::repeat_with(Worker::new_lifo)
        .take(local_num)
        .collect();
    let stealers: Vec<_> = workers.iter().map(Worker::stealer).collect();
    let local_queues = workers
        .into_iter()
        .enumerate()
        .map(|(self_index, local_queue)| {
            let mut stealers: Vec<_> = stealers
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != self_index)
                .map(|(_, stealer)| stealer.clone())
                .collect();
            // Steal with a random start to avoid imbalance.
            stealers.shuffle(&mut thread_rng());
            LocalQueue {
                local_queue,
                injector: injector.clone(),
                stealers,
            }
        })
        .collect();

    (TaskInjector(injector), local_queues)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::queue::Extras;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::thread;
    use std::time::Duration;

    #[derive(Debug)]
    struct MockCell {
        value: i32,
        extras: Extras,
    }

    impl MockCell {
        fn new(value: i32) -> Self {
            MockCell {
                value,
                extras: Extras::single_level(),
            }
        }
    }

    impl TaskCell for MockCell {
        fn mut_extras(&mut self) -> &mut Extras {
            &mut self.extras
        }
    }

    #[test]
    fn test_schedule_time_is_set() {
        const SLEEP_DUR: Duration = Duration::from_millis(5);

        let (injector, mut locals) = super::create(1);
        injector.push(MockCell::new(0));
        thread::sleep(SLEEP_DUR);
        let schedule_time = locals[0].pop().unwrap().schedule_time;
        assert!(schedule_time.elapsed() >= SLEEP_DUR);
    }

    #[test]
    fn test_pop_by_stealing_injector() {
        let (injector, mut locals) = super::create(3);
        for i in 0..100 {
            injector.push(MockCell::new(i));
        }
        let sum: i32 = (0..100)
            .map(|_| locals[2].pop().unwrap().task_cell.value)
            .sum();
        assert_eq!(sum, (0..100).sum());
        assert!(locals.iter_mut().all(|c| c.pop().is_none()));
    }

    #[test]
    fn test_pop_by_steal_others() {
        let (injector, mut locals) = super::create(3);
        for i in 0..50 {
            injector.push(MockCell::new(i));
        }
        assert!(injector.0.steal_batch(&locals[0].local_queue).is_success());
        for i in 50..100 {
            injector.push(MockCell::new(i));
        }
        assert!(injector.0.steal_batch(&locals[1].local_queue).is_success());
        let sum: i32 = (0..100)
            .map(|_| locals[2].pop().unwrap().task_cell.value)
            .sum();
        assert_eq!(sum, (0..100).sum());
        assert!(locals.iter_mut().all(|c| c.pop().is_none()));
    }

    #[test]
    fn test_pop_concurrently() {
        let (injector, locals) = super::create(3);
        for i in 0..10_000 {
            injector.push(MockCell::new(i));
        }
        let sum = Arc::new(AtomicI32::new(0));
        let handles: Vec<_> = locals
            .into_iter()
            .map(|mut consumer| {
                let sum = sum.clone();
                thread::spawn(move || {
                    while let Some(pop) = consumer.pop() {
                        sum.fetch_add(pop.task_cell.value, Ordering::SeqCst);
                    }
                })
            })
            .collect();
        for handle in handles {
            let _ = handle.join();
        }
        assert_eq!(sum.load(Ordering::SeqCst), (0..10_000).sum());
    }
}
