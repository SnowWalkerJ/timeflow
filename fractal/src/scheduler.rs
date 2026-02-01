use crossbeam::channel::{Receiver, Sender, unbounded};
use crossbeam::thread::{Scope, ScopedJoinHandle};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Duration;

use super::definition::*;

pub(crate) struct TaskInfo {
    ascandants: HashSet<Id>,
    descandants: HashSet<Id>,
    task: Option<Box<dyn ErasedTask>>,
}

impl TaskInfo {
    pub fn new(task: Box<dyn ErasedTask>, dependency: HashSet<Id>) -> Self {
        Self {
            ascandants: dependency,
            descandants: HashSet::new(),
            task: Some(task),
        }
    }
}

type TaskMap = HashMap<Id, TaskInfo>;

#[derive(Clone)]
pub struct SubScheduler {
    id: Arc<AtomicU64>,
    tasks: Arc<RwLock<TaskMap>>,
    task_tx: Sender<(Id, Box<dyn ErasedTask>)>,
    results: Arc<RwLock<HashMap<Id, ResultBox>>>,
}

impl SubScheduler {
    pub(crate) fn new(task_tx: Sender<(Id, Box<dyn ErasedTask>)>) -> Self {
        Self {
            id: Arc::new(AtomicU64::new(0)),
            tasks: Arc::new(RwLock::new(TaskMap::new())),
            task_tx,
            results: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub fn submit(
        &mut self,
        task: Box<dyn ErasedTask>,
        dependency: HashSet<TaskRef>,
    ) -> Result<TaskRef, SubmitError> {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        {
            let mut tasks = self.tasks.write().unwrap();
            for dep in dependency.iter() {
                if !(*tasks).contains_key(&dep.id) {
                    return Err(SubmitError::new(dep.id));
                }
                tasks.get_mut(&dep.id).unwrap().descandants.insert(id);
            }
            tasks.insert(
                id,
                TaskInfo::new(task, dependency.iter().map(|d| d.id).collect()),
            );
        }
        let dependency_resolved = {
            let results = self.results.read().unwrap();
            let mut dependency_resolved = true;
            for dep in dependency.iter() {
                if !(*results).contains_key(&dep.id) {
                    dependency_resolved = false;
                }
            }
            dependency_resolved
        };
        if dependency_resolved {
            self._submit(id);
        }
        Ok(TaskRef::new(id))
    }

    pub fn outcome(&mut self, id: Id, result: ResultBox) {
        {
            let mut results = self.results.write().unwrap();
            results.insert(id, result);
        }
        let runnable_descandant_ids = {
            let mut runnable_descandant_ids = HashSet::new();
            let mut tasks = self.tasks.write().unwrap();
            for descandant_id in tasks.get(&id).unwrap().descandants.clone().iter() {
                let descandant = tasks.get_mut(descandant_id).unwrap();
                descandant.ascandants.remove(&id);
                if descandant.ascandants.is_empty() {
                    runnable_descandant_ids.insert(*descandant_id);
                }
            }
            runnable_descandant_ids
        };
        for descandant_id in runnable_descandant_ids {
            self._submit(descandant_id);
        }
    }

    pub fn get<T: Any + Send + Sync + 'static>(
        &self,
        taskref: &TaskRef,
    ) -> ReadLockedResult<'_, T> {
        let guard = self.results.read().unwrap();
        ReadLockedResult::new(taskref.id, guard)
    }

    fn _submit(&mut self, id: Id) {
        let task = {
            let mut tasks = self.tasks.write().unwrap();
            let item = tasks.get_mut(&id).unwrap();
            std::mem::take(&mut item.task).unwrap()
        };
        self.task_tx.send((id, task)).unwrap();
    }
}

pub struct ReadLockedResult<'a, T: Any> {
    id: Id,
    guard: RwLockReadGuard<'a, HashMap<Id, ResultBox>>,
    phantom: PhantomData<T>,
}

impl<'a, T: Any> ReadLockedResult<'a, T> {
    pub fn new(id: Id, guard: RwLockReadGuard<'a, HashMap<Id, ResultBox>>) -> Self {
        Self {
            id,
            guard,
            phantom: PhantomData,
        }
    }
    pub fn get(&self) -> Option<&T> {
        self.guard.get(&self.id)?.downcast_ref::<T>()
    }
}

pub struct MyScheduler<'env, 'scope> {
    scope: &'scope Scope<'env>,
    exit_signal: Arc<AtomicBool>,
    threads: Vec<ScopedJoinHandle<'scope, ()>>,

    sub_scheduler: SubScheduler,
}

impl<'env, 'scope> MyScheduler<'env, 'scope> {
    pub fn new(scope: &'scope Scope<'env>) -> Self {
        let nthreads = std::thread::available_parallelism().unwrap().get();
        let exit_signal = Arc::new(AtomicBool::new(false));
        let (task_tx, task_rx) = unbounded::<(Id, Box<dyn ErasedTask + 'static>)>();
        let sub_scheduler = SubScheduler::new(task_tx);
        let mut threads = Vec::with_capacity(nthreads);
        for _ in 0..nthreads {
            let exist_signal = Arc::clone(&exit_signal);
            let task_rx = task_rx.clone();
            let sub_scheduler = sub_scheduler.clone();
            let thread = scope.spawn(move |_| {
                let mut sub_scheduler = sub_scheduler;
                loop {
                    if exist_signal.load(Ordering::Acquire) {
                        break;
                    }
                    if let Ok((id, mut task)) = task_rx.recv_timeout(Duration::from_secs_f32(0.1)) {
                        print!("Worker received task!!!!!!!");
                        let result = task.compute(&mut sub_scheduler);
                        sub_scheduler.outcome(id, result);
                    }
                }
            });
            threads.push(thread);
        }
        Self {
            scope,
            exit_signal,
            threads,
            sub_scheduler,
        }
    }
    pub fn submit(
        &mut self,
        task: Box<dyn ErasedTask + 'static>,
        dependency: HashSet<TaskRef>,
    ) -> Result<TaskRef, SubmitError> {
        self.sub_scheduler.submit(task, dependency)
    }
    pub fn get<T: Any + Send + Sync + 'static>(
        &self,
        taskref: &TaskRef,
    ) -> ReadLockedResult<'_, T> {
        self.sub_scheduler.get::<T>(taskref)
    }
}

impl<'env, 'scope> Drop for MyScheduler<'env, 'scope> {
    fn drop(&mut self) {
        self.exit_signal.store(true, Ordering::Release);
        let threads = std::mem::take(&mut self.threads);
        for thread in threads.into_iter() {
            thread.join().unwrap();
        }
        println!("Scheduler dropped!!!!!!!!!!");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_scheduler() {
        struct Const {
            pub(crate) value: f32,
        }
        impl Task for Const {
            type Output = f32;
            fn compute(self, _scheduler: &mut SubScheduler) -> Self::Output {
                self.value
            }
        }

        struct Add {
            lhs: TaskRef,
            rhs: TaskRef,
        }
        impl Task for Add {
            type Output = f32;
            fn compute(self, scheduler: &mut SubScheduler) -> Self::Output {
                let lhs = *scheduler.get::<f32>(&self.lhs).get().unwrap();
                let rhs = *scheduler.get::<f32>(&self.rhs).get().unwrap();
                lhs + rhs
            }
        }
        crossbeam::scope(|s| {
            let mut scheduler = MyScheduler::new(s);
            let task_ref1 = scheduler
                .submit(
                    Box::new(WrappedTask::new(Const { value: 0.1 })),
                    HashSet::new(),
                )
                .unwrap();
            let task_ref2 = scheduler
                .submit(
                    Box::new(WrappedTask::new(Const { value: 0.2 })),
                    HashSet::new(),
                )
                .unwrap();
            let dependency = {
                let mut dep = HashSet::new();
                dep.insert(task_ref1);
                dep.insert(task_ref2);
                dep
            };
            let task_ref3 = scheduler
                .submit(
                    Box::new(WrappedTask::new(Add {
                        lhs: task_ref1,
                        rhs: task_ref2,
                    })),
                    dependency,
                )
                .unwrap();
            std::thread::sleep(Duration::from_secs(1));
            let result = *scheduler.get::<f32>(&task_ref3).get().unwrap();
            assert_eq!(result, 0.3f32);
        })
        .unwrap();
    }
}
