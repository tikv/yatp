// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A simple task queue.
//!
//! This task queue requires [`TaskCell`] to contain [`SimpleQueueExtras`]
//! as its extras. The instant when the task cell is pushed into the queue
//! is recorded in the extras.

use super::{LocalQueue, Pop, TaskCell, TaskInjector};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use std::iter;
use std::sync::Arc;
use std::time::Instant;

/// The injector of a simple task queue.
pub struct SimpleQueueInjector<T>(Arc<Injector<T>>);

impl<T: TaskCell> Clone for SimpleQueueInjector<T> {
    fn clone(&self) -> Self {
        SimpleQueueInjector(self.0.clone())
    }
}

/// The extras for the task cells pushed into a simple task queue.
///
/// [`Default::default`] can be used to create a [`SimpleQueueExtras`] for
/// a task cell.
#[derive(Debug, Default, Clone, Copy)]
pub struct SimpleQueueExtras {
    /// The instant when the task cell is pushed to the queue.
    schedule_time: Option<Instant>,
}

fn set_schedule_time<T>(task_cell: &mut T)
where
    T: TaskCell<Extras = SimpleQueueExtras>,
{
    task_cell.mut_extras().schedule_time = Some(Instant::now());
}

impl<T> TaskInjector for SimpleQueueInjector<T>
where
    T: TaskCell<Extras = SimpleQueueExtras>,
{
    type TaskCell = T;

    /// Pushes the task cell to the queue. The schedule time in the extras is
    /// assigned to be now.
    fn push(&self, mut task_cell: Self::TaskCell) {
        set_schedule_time(&mut task_cell);
        self.0.push(task_cell);
    }
}

/// The local queue of a simple task queue.
pub struct SimpleQueueLocal<T> {
    local_queue: Worker<T>,
    injector: Arc<Injector<T>>,
    stealers: Vec<Stealer<T>>,
    self_index: usize,
}

impl<T> LocalQueue for SimpleQueueLocal<T>
where
    T: TaskCell<Extras = SimpleQueueExtras>,
{
    type TaskCell = T;

    fn push(&mut self, mut task_cell: Self::TaskCell) {
        set_schedule_time(&mut task_cell);
        self.local_queue.push(task_cell);
    }

    fn pop(&mut self) -> Option<Pop<Self::TaskCell>> {
        fn into_pop<T>(mut t: T, from_local: bool) -> Pop<T>
        where
            T: TaskCell<Extras = SimpleQueueExtras>,
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
        let mut need_retry;
        loop {
            need_retry = false;
            match self.injector.steal_batch_and_pop(&self.local_queue) {
                Steal::Success(t) => return Some(into_pop(t, false)),
                Steal::Retry => need_retry = true,
                _ => {}
            }
            for (i, stealer) in self.stealers.iter().enumerate() {
                if i == self.self_index {
                    continue;
                }
                match stealer.steal_batch_and_pop(&self.local_queue) {
                    Steal::Success(t) => return Some(into_pop(t, false)),
                    Steal::Retry => need_retry = true,
                    _ => {}
                }
            }
            if !need_retry {
                break None;
            }
        }
    }
}

/// Creates a simple task queue with `local_num` local queues.
pub fn create<T>(local_num: usize) -> (SimpleQueueInjector<T>, Vec<SimpleQueueLocal<T>>) {
    let injector = Arc::new(Injector::new());
    let workers: Vec<_> = iter::repeat_with(Worker::new_lifo)
        .take(local_num)
        .collect();
    let stealers: Vec<_> = workers.iter().map(Worker::stealer).collect();
    let local_queues = workers
        .into_iter()
        .enumerate()
        .map(|(self_index, local_queue)| SimpleQueueLocal {
            local_queue,
            injector: injector.clone(),
            stealers: stealers.clone(),
            self_index,
        })
        .collect();

    (SimpleQueueInjector(injector), local_queues)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicI32, Ordering};
    use std::thread;
    use std::time::Duration;

    #[derive(Debug)]
    struct MockCell {
        value: i32,
        extras: SimpleQueueExtras,
    }

    impl MockCell {
        fn new(value: i32) -> Self {
            MockCell {
                value,
                extras: Default::default(),
            }
        }
    }

    impl TaskCell for MockCell {
        type Extras = SimpleQueueExtras;

        fn mut_extras(&mut self) -> &mut SimpleQueueExtras {
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
