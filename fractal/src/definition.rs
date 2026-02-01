use std::any::Any;
use std::ops::Sub;
use std::time::Duration;

use crate::scheduler::SubScheduler;
pub type Id = u64;

pub trait Task: Send + Sync {
    type Output: Send + Any;
    fn compute(self, scheduler: &mut SubScheduler) -> Self::Output;
}

pub type ResultBox = Box<dyn Any + Sync + Send + 'static>;

pub trait ErasedTask: Send + Sync {
    fn compute(&mut self, scheduler: &mut SubScheduler) -> ResultBox;
}

pub struct WrappedTask<T, R: Send + Any>
where
    T: Task<Output = R>,
{
    task: Option<T>,
}

impl<T, R> WrappedTask<T, R>
where
    T: Task<Output = R>,
    R: Any + Sync + Send + 'static,
{
    pub fn new(task: T) -> Self {
        Self { task: Some(task) }
    }
}

impl<T, R: Any + Sync + Send + 'static> ErasedTask for WrappedTask<T, R>
where
    T: Task<Output = R>,
{
    fn compute(&mut self, scheduler: &mut SubScheduler) -> ResultBox {
        let task = std::mem::take(&mut self.task);
        Box::new(task.unwrap().compute(scheduler))
    }
}

#[derive(Clone, Copy)]
pub struct TaskRef {
    pub(crate) id: Id,
}

impl TaskRef {
    pub(crate) fn new(id: Id) -> Self {
        Self { id }
    }
}

pub trait Scheduler {
    fn submit<R: Send + Any, T: Task<Output = R> + 'static>(
        &mut self,
        task: T,
        dependencies: Vec<TaskRef>,
    ) -> Result<TaskRef, SubmitError>;

    fn get<T: Send + Any>(&self, task_ref: &TaskRef) -> Option<&T>;
    fn wait(&self, task_ref: &TaskRef, timeout: Option<Duration>) -> bool;
}

#[derive(Debug)]
pub struct SubmitError {
    id: Id,
}

impl SubmitError {
    pub(crate) fn new(id: Id) -> Self {
        Self { id }
    }
}

impl std::fmt::Display for SubmitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Can't find dependency task with id `{}`",
            self.id
        )
    }
}

impl std::error::Error for SubmitError {}
