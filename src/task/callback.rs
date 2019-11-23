// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A [`FnOnce`] or [`FnMut`] closure.

use crate::LocalSpawn;

use std::marker::PhantomData;

/// A callback task, which is either a [`FnOnce`] or a [`FnMut`].
pub enum Task<Spawn>
where
    Spawn: LocalSpawn,
{
    /// A [`FnOnce`] task.
    Once(Box<dyn FnOnce(&mut Handle<'_, Spawn>) + Send>),
    /// A [`FnMut`] task.
    Mut(Box<dyn FnMut(&mut Handle<'_, Spawn>) + Send>),
}

impl<Spawn> Task<Spawn>
where
    Spawn: LocalSpawn,
{
    /// Creates a [`FnOnce`] task.
    pub fn new_once(t: impl FnOnce(&mut Handle<'_, Spawn>) + Send + 'static) -> Self {
        Task::Once(Box::new(t))
    }

    /// Creates a [`FnMut`] task.
    pub fn new_mut(t: impl FnMut(&mut Handle<'_, Spawn>) + Send + 'static) -> Self {
        Task::Mut(Box::new(t))
    }
}

/// Handle passed to the task closure.
///
/// It can be used to spawn new tasks or control whether this task should be
/// rerun.
pub struct Handle<'a, Spawn>
where
    Spawn: LocalSpawn,
{
    spawn: &'a mut Spawn,
    ctx: &'a Spawn::TaskContext,
    rerun: bool,
}

impl<'a, Spawn> Handle<'a, Spawn>
where
    Spawn: LocalSpawn<Task = Task<Spawn>>,
{
    /// Spawns a [`FnOnce`] to the thread pool with the same task context.
    pub fn spawn_once(&mut self, t: impl FnOnce(&mut Handle<'_, Spawn>) + Send + 'static) {
        self.spawn.spawn_ctx(Task::new_once(t), self.ctx);
    }

    /// Spawns a [`FnMut`] to the thread pool with the same task context.
    pub fn spawn_mut(&mut self, t: impl FnMut(&mut Handle<'_, Spawn>) + Send + 'static) {
        self.spawn.spawn_ctx(Task::new_mut(t), self.ctx);
    }

    /// Spawns a [`FnOnce`] to the thread pool with the given task context.
    pub fn spawn_once_ctx(
        &mut self,
        t: impl FnOnce(&mut Handle<'_, Spawn>) + Send + 'static,
        ctx: &Spawn::TaskContext,
    ) {
        self.spawn.spawn_ctx(Task::new_once(t), ctx);
    }

    /// Spawns a [`FnMut`] to the thread pool with the given task context.
    pub fn spawn_mut_ctx(
        &mut self,
        t: impl FnMut(&mut Handle<'_, Spawn>) + Send + 'static,
        ctx: &Spawn::TaskContext,
    ) {
        self.spawn.spawn_ctx(Task::new_mut(t), ctx);
    }

    /// Gets the task context.
    pub fn context(&self) -> &Spawn::TaskContext {
        &self.ctx
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
pub struct Runner<Spawn> {
    max_inplace_spin: usize,
    _phantom: PhantomData<Spawn>,
}

impl<Spawn> Runner<Spawn> {
    /// Creates a new runner with given `max_inplace_spin`.
    pub fn new(max_inplace_spin: usize) -> Self {
        Self {
            max_inplace_spin,
            ..Default::default()
        }
    }

    ///  Sets `max_inplace_spin`.
    pub fn set_max_inplace_spin(&mut self, max_inplace_spin: usize) {
        self.max_inplace_spin = max_inplace_spin;
    }
}

impl<Spawn> Default for Runner<Spawn> {
    fn default() -> Self {
        Runner {
            max_inplace_spin: 4,
            _phantom: PhantomData,
        }
    }
}

impl<Spawn> crate::Runner for Runner<Spawn>
where
    Spawn: LocalSpawn<Task = Task<Spawn>>,
{
    type Task = Task<Spawn>;
    type Spawn = Spawn;

    fn handle(
        &mut self,
        spawn: &mut Spawn,
        mut task: Task<Spawn>,
        ctx: &<Self::Spawn as LocalSpawn>::TaskContext,
    ) -> bool {
        let mut handle = Handle {
            spawn,
            ctx,
            rerun: false,
        };
        match task {
            Task::Mut(ref mut r) => {
                let mut tried_times = 0;
                loop {
                    r(&mut handle);
                    if !handle.rerun {
                        return true;
                    }
                    tried_times += 1;
                    if tried_times == self.max_inplace_spin {
                        break;
                    }
                    handle.rerun = false;
                }
            }
            Task::Once(r) => {
                r(&mut handle);
                return true;
            }
        }
        spawn.spawn_ctx(task, ctx);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RemoteSpawn, Runner as _};

    use std::sync::mpsc;

    #[derive(Default)]
    struct MockSpawn {
        spawn_times: usize,
    }

    impl LocalSpawn for MockSpawn {
        type Task = Task<MockSpawn>;
        type TaskContext = ();
        type Remote = MockSpawn;

        fn spawn_ctx(&mut self, _t: impl Into<Self::Task>, _ctx: &()) {
            self.spawn_times += 1;
        }

        fn remote(&self) -> Self::Remote {
            unimplemented!()
        }
    }

    impl RemoteSpawn for MockSpawn {
        type Task = Task<MockSpawn>;
        type SpawnOption = ();

        fn spawn_opt(&self, _t: impl Into<Self::Task>, _opt: &()) {
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
            Task::new_once(move |_| {
                tx.send(42).unwrap();
            }),
            &(),
        );
        assert_eq!(rx.recv().unwrap(), 42);
    }

    #[test]
    fn test_mut_no_spin() {
        let mut runner = Runner::new(2);
        let mut spawn = MockSpawn::default();
        let (tx, rx) = mpsc::channel();

        let mut times = 0;
        runner.handle(
            &mut spawn,
            Task::new_mut(move |handle| {
                tx.send(42).unwrap();
                times += 1;
                if times < 2 {
                    handle.set_rerun(true);
                }
            }),
            &(),
        );
        assert_eq!(rx.recv().unwrap(), 42);
        assert_eq!(rx.recv().unwrap(), 42);
        assert_eq!(spawn.spawn_times, 0);
        assert!(rx.recv().is_err());
    }

    #[test]
    fn test_mut_spin() {
        let mut runner = Runner::new(2);
        let mut spawn = MockSpawn::default();
        let (tx, rx) = mpsc::channel();

        let mut times = 0;
        runner.handle(
            &mut spawn,
            Task::new_mut(move |handle| {
                tx.send(42).unwrap();
                times += 1;
                if times < 3 {
                    handle.set_rerun(true);
                }
            }),
            &(),
        );
        assert_eq!(rx.recv().unwrap(), 42);
        assert_eq!(rx.recv().unwrap(), 42);
        assert_eq!(spawn.spawn_times, 1);
        assert!(rx.recv().is_err());
    }
}
