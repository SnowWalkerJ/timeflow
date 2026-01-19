use std::any::Any;
use std::collections::HashSet;
use std::rc::Rc;

use crate::scheduler;

pub type KeyType = u128;

pub trait Scheduler {
    fn submit(&mut self, task: Box<dyn WrappedComputable>, dependency: &[KeyType]);
    fn get_result(&self, key: KeyType) -> Option<&dyn Any>;
    fn run(&mut self);
}

#[derive(Debug, Clone, Copy)]
pub enum Status {
    DepNotFulfilled,
    InQueue,
    Running,
    Completed,
}

pub trait Computable {
    type Output;
    fn get_uri(&self) -> u128;
    fn submit<'a, 'b: 'a>(&self, submitter: &'a mut Submitter<'b>) -> HashSet<KeyType>;
    fn execute(&self, receiver: &Receiver) -> Box<Self::Output>;
}

pub trait WrappedComputable {
    fn get_uri(&self) -> KeyType;
    fn execute(&self, receiver: &Receiver) -> Box<dyn Any>;
}

pub struct Wrapped<T>(Rc<dyn Computable<Output = T>>);

impl<T: Any> WrappedComputable for Wrapped<T> {
    fn get_uri(&self) -> KeyType {
        self.0.get_uri()
    }
    fn execute(&self, receiver: &Receiver) -> Box<dyn Any> {
        self.0.execute(receiver)
    }
}

impl<T: 'static> Wrapped<T> {
    pub fn new(node: Rc<dyn Computable<Output = T>>) -> Self {
        Self(node)
    }
}

pub struct Receiver<'a>(&'a dyn Scheduler);

impl<'a> Receiver<'a> {
    pub fn new(scheduler: &'a dyn Scheduler) -> Self {
        Self(scheduler)
    }
    pub fn get_result<T: 'static>(&self, uri: KeyType) -> Option<&T> {
        let result_any = self.0.get_result(uri);
        result_any?.downcast_ref()
    }
}

pub struct Submitter<'a>(&'a mut dyn Scheduler);

impl<'a> Submitter<'a> {
    pub fn new(scheduler: &'a mut dyn Scheduler) -> Self {
        Self(scheduler)
    }
    pub fn submit<T: 'static>(&mut self, task: Rc<dyn Computable<Output = T>>) {
        let dependencies = task.submit(self);
        let wrapped = Box::new(Wrapped::new(task));
        self.0
            .submit(wrapped, &dependencies.iter().copied().collect::<Vec<_>>());
    }
}
