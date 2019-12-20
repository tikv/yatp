// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A [`FnOnce`] or [`FnMut`] closure.

use crate::queue::Extras;
use crate::pool::Local;

/// A callback task, which is either a [`FnOnce`] or a [`FnMut`].
pub enum Task {
    /// A [`FnOnce`] task.
    Once(Box<dyn FnOnce(&mut Handle<'_>) + Send>),
    /// A [`FnMut`] task.
    Mut(Box<dyn FnMut(&mut Handle<'_>) + Send>),
}

impl Task {
    /// Creates a [`FnOnce`] task.
    pub fn new_once(t: impl FnOnce(&mut Handle<'_>) + Send + 'static) -> Self {
        Task::Once(Box::new(t))
    }

    /// Creates a [`FnMut`] task.
    pub fn new_mut(t: impl FnMut(&mut Handle<'_>) + Send + 'static) -> Self {
        Task::Mut(Box::new(t))
    }
}

/// The task cell for callback tasks.
pub struct TaskCell {
    /// The callback task.
    pub task: Task,
    /// Extra information about the task.
    pub extras: Extras,
}

impl crate::queue::TaskCell for TaskCell {
    fn mut_extras(&mut self) -> &mut Extras {
        &mut self.extras
    }
}

/// Handle passed to the task closure.
///
/// It can be used to spawn new tasks or control whether this task should be
/// rerun.
pub struct Handle<'a> {
    spawn: &'a mut Local<TaskCell>,
    rerun: bool,
}

impl<'a> Handle<'a> {
    /// Spawns a [`FnOnce`] to the thread pool.
    pub fn spawn_once(
        &mut self,
        t: impl FnOnce(&mut Handle<'_>) + Send + 'static,
        extras: Extras,
    ) {
        self.spawn.spawn(TaskCell {
            task: Task::new_once(t),
            extras,
        });
    }

    /// Spawns a [`FnMut`] to the thread pool.
    pub fn spawn_mut(
        &mut self,
        t: impl FnMut(&mut Handle<'_>) + Send + 'static,
        extras: Extras,
    ) {
        self.spawn.spawn(TaskCell {
            task: Task::new_mut(t),
            extras,
        });
    }

    /// Sets whether this task should be rerun later.
    pub fn set_rerun(&mut self, rerun: bool) {
        self.rerun = rerun;
    }
}

/// Callback task runner.
///
/// It's possible that a task can't be finished in a single execution and needs
/// to be rerun. `max_inspace_spin` is the maximum times a task is rerun at once
/// before being put back to the thread pool.
pub struct Runner {
    max_inplace_spin: usize,
}

impl Runner {
    /// Creates a new runner with given `max_inplace_spin`.
    pub fn new(max_inplace_spin: usize) -> Self {
        Self {
            max_inplace_spin,
            ..Default::default()
        }
    }

    /// Sets `max_inplace_spin`.
    pub fn set_max_inplace_spin(&mut self, max_inplace_spin: usize) {
        self.max_inplace_spin = max_inplace_spin;
    }
}

impl Default for Runner {
    fn default() -> Self {
        Runner {
            max_inplace_spin: 3,
        }
    }
}

impl Clone for Runner {
    fn clone(&self) -> Runner {
        Runner {
            max_inplace_spin: self.max_inplace_spin,
        }
    }
}

impl crate::Runner for Runner {
    type TaskCell = TaskCell;

    fn handle(&mut self, spawn: &mut Local<TaskCell>, mut task_cell: TaskCell) -> bool {
        let mut handle = Handle {
            spawn,
            rerun: false,
        };
        match task_cell.task {
            Task::Mut(ref mut r) => {
                let mut rerun_times = 0;
                loop {
                    r(&mut handle);
                    if !handle.rerun {
                        return true;
                    }
                    if rerun_times >= self.max_inplace_spin {
                        break;
                    }
                    rerun_times += 1;
                    handle.rerun = false;
                }
            }
            Task::Once(r) => {
                r(&mut handle);
                return true;
            }
        }
        spawn.spawn(task_cell);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Runner as _;

    use std::sync::mpsc;

    #[derive(Default)]
    struct MockSpawn {
        spawn_times: usize,
    }

    impl MockSpawn {
        fn spawn(&mut self, _t: TaskCell) {
            self.spawn_times += 1;
        }

        fn remote(&self) -> Self::Remote {
            unimplemented!()
        }
    }

    #[test]
    fn test_once() {
        let mut runner = Runner::default();
        let mut spawn = MockSpawn::default();
        let (tx, rx) = mpsc::channel();
        runner.handle(
            &mut spawn,
            TaskCell {
                task: Task::new_once(move |_| {
                    tx.send(42).unwrap();
                }),
                extras: Default::default(),
            },
        );
        assert_eq!(rx.recv().unwrap(), 42);
    }

    #[test]
    fn test_mut_no_respawn() {
        let mut runner = Runner::new(1);
        let mut spawn = MockSpawn::default();
        let (tx, rx) = mpsc::channel();

        let mut times = 0;
        runner.handle(
            &mut spawn,
            TaskCell {
                task: Task::new_mut(move |handle| {
                    tx.send(42).unwrap();
                    times += 1;
                    if times < 2 {
                        handle.set_rerun(true);
                    }
                }),
                extras: Default::default(),
            },
        );
        assert_eq!(rx.recv().unwrap(), 42);
        assert_eq!(rx.recv().unwrap(), 42);
        assert_eq!(spawn.spawn_times, 0);
        assert!(rx.recv().is_err());
    }

    #[test]
    fn test_mut_respawn() {
        let mut runner = Runner::new(1);
        let mut spawn = MockSpawn::default();
        let (tx, rx) = mpsc::channel();

        let mut times = 0;
        runner.handle(
            &mut spawn,
            TaskCell {
                task: Task::new_mut(move |handle| {
                    tx.send(42).unwrap();
                    times += 1;
                    if times < 3 {
                        handle.set_rerun(true);
                    }
                }),
                extras: Default::default(),
            },
        );
        assert_eq!(rx.recv().unwrap(), 42);
        assert_eq!(rx.recv().unwrap(), 42);
        assert_eq!(spawn.spawn_times, 1);
        assert!(rx.recv().is_err());
    }
}
