use std::any::Any;
use std::collections::HashSet;
use std::marker::PhantomData;
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

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RawTaskRef(pub(crate) Id);

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TypedTaskRef<T> {
    raw: RawTaskRef,
    phantom: PhantomData<*const T>,
}

impl<T> std::ops::Deref for TypedTaskRef<T> {
    type Target = RawTaskRef;
    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl RawTaskRef {
    pub(crate) fn new(id: Id) -> Self {
        Self(id)
    }
}

pub trait TaskRef {
    fn get_ref(&self) -> RawTaskRef;
}

impl<T> TaskRef for TypedTaskRef<T> {
    fn get_ref(&self) -> RawTaskRef {
        self.raw
    }
}

impl TaskRef for RawTaskRef {
    fn get_ref(&self) -> RawTaskRef {
        self.clone()
    }
}

pub trait Scheduler {
    fn submit<R: Send + Any + Sync + 'static, T: Task<Output = R> + 'static>(
        &mut self,
        task: T,
        dependencies: HashSet<RawTaskRef>,
    ) -> Result<TypedTaskRef<R>, SubmitError> {
        let task = Box::new(WrappedTask::new(task));
        let raw_task_ref = self.submit_erased(task, dependencies)?;
        Ok(TypedTaskRef {
            raw: raw_task_ref,
            phantom: PhantomData,
        })
    }

    fn submit_erased(
        &mut self,
        task: Box<dyn ErasedTask>,
        dependency: HashSet<RawTaskRef>,
    ) -> Result<RawTaskRef, SubmitError>;

    fn get<T: Sync + Send + Any + 'static>(&self, task_ref: &TypedTaskRef<T>) -> Option<&T>;
    fn get_erased(&self, task_ref: RawTaskRef) -> Option<&ResultBox>;
    fn wait(&self, task_ref: &RawTaskRef, timeout: Option<Duration>) -> bool;
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

pub trait CompleteTask {
    type Output;
    fn task(self) -> impl Task<Output = Self::Output>;
    fn dependency(&self) -> HashSet<RawTaskRef>;
}
