use crossbeam::channel::{Receiver, Sender, unbounded};
use crossbeam::thread::{Scope, ScopedJoinHandle};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::process::exit;
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
    results: Arc<RwLock<HashMap<Id, Arc<ResultType>>>>,
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
        dependency: HashSet<RawTaskRef>,
    ) -> Result<RawTaskRef, SubmitError> {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        {
            let mut tasks = self.tasks.write().unwrap();
            for dep in dependency.iter() {
                if !(*tasks).contains_key(&dep.0) {
                    return Err(SubmitError::new(dep.0));
                }
                tasks.get_mut(&dep.0).unwrap().descandants.insert(id);
            }
            tasks.insert(
                id,
                TaskInfo::new(task, dependency.iter().map(|d| d.0).collect()),
            );
        }
        let dependency_resolved = {
            let results = self.results.read().unwrap();
            dependency.iter().all(|dep| (*results).contains_key(&dep.0))
        };
        if dependency_resolved {
            self._submit(id);
        }
        Ok(RawTaskRef::new(id))
    }

    pub fn outcome(&mut self, id: Id, result: ResultBox) {
        {
            let mut results = self.results.write().unwrap();
            results.insert(id, Arc::new(RwLock::new(result)));
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

    pub fn get(&self, taskref: &RawTaskRef) -> Option<Arc<ResultType>> {
        let guard = self.results.read().ok()?;
        Some(Arc::clone(guard.get(&taskref.0)?))
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

pub fn run<F, T>(f: F) -> T
where
    F: FnOnce(SubScheduler) -> T,
{
    let nthreads = std::thread::available_parallelism().unwrap().get();
    let exit_signal = Arc::new(AtomicBool::new(false));
    let (task_tx, task_rx) = unbounded::<(Id, Box<dyn ErasedTask + 'static>)>();
    let sub_scheduler = SubScheduler::new(task_tx);
    crossbeam::scope(|scope| {
        for _ in 0..nthreads {
            let exist_signal = Arc::clone(&exit_signal);
            let task_rx = task_rx.clone();
            let sub_scheduler = sub_scheduler.clone();
            scope.spawn(move |_| {
                let mut sub_scheduler = sub_scheduler;
                loop {
                    if exist_signal.load(Ordering::Acquire) {
                        break;
                    }
                    if let Ok((id, mut task)) = task_rx.recv_timeout(Duration::from_secs_f32(0.1)) {
                        println!("Worker received task {} !!!!!!!", id);
                        let result = task.compute(&mut sub_scheduler);
                        sub_scheduler.outcome(id, result);
                    }
                }
            });
        }
        let result = f(sub_scheduler);
        exit_signal.store(true, Ordering::Release);
        result
    })
    .unwrap()
}

#[macro_export]
macro_rules! submit {
    ($scheduler:ident, $task:expr) => {{
        let complete_task = $task;
        let dependency = complete_task.dependency();
        let task = complete_task.task();
        let raw_task_ref = $scheduler.submit(Box::new(WrappedTask::new(task)), dependency);
        raw_task_ref
    }};
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
        impl CompleteTask for Const {
            type Output = f32;
            fn dependency(&self) -> HashSet<RawTaskRef> {
                HashSet::new()
            }
            fn task(self) -> impl Task<Output = Self::Output> {
                self
            }
        }

        struct Add {
            lhs: RawTaskRef,
            rhs: RawTaskRef,
        }
        impl Task for Add {
            type Output = f32;
            fn compute(self, scheduler: &mut SubScheduler) -> Self::Output {
                let lhs: f32 = *scheduler.get(&self.lhs).unwrap().downcast_ref().unwrap();
                let rhs: f32 = *scheduler.get(&self.rhs).unwrap().downcast_ref().unwrap();
                lhs + rhs
            }
        }
        impl CompleteTask for Add {
            type Output = f32;
            fn dependency(&self) -> HashSet<RawTaskRef> {
                let mut deps = HashSet::new();
                deps.insert(self.lhs);
                deps.insert(self.rhs);
                deps
            }
            fn task(self) -> impl Task<Output = Self::Output> {
                self
            }
        }
        run(|mut scheduler| {
            let task_ref1 = submit!(scheduler, Const { value: 0.1 }).unwrap();
            let task_ref2 = submit!(scheduler, Const { value: 0.2 }).unwrap();
            let task_ref3 = submit!(
                scheduler,
                Add {
                    lhs: task_ref1,
                    rhs: task_ref2,
                }
            )
            .unwrap();

            let task_ref4 = submit!(scheduler, Const { value: 0.2 }).unwrap();
            std::thread::sleep(Duration::from_secs(1));
            let result: f32 = *scheduler.get(&task_ref3).unwrap().downcast_ref().unwrap();
            let _ = *scheduler.get(&task_ref4).unwrap();
            assert_eq!(result, 0.3f32);
        });
    }
}
