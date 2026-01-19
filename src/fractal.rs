use std::cell::RefCell;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::sync::Arc;

type KeyType = u128;

pub trait Scheduler {
    fn submit(&mut self, task: Box<dyn WrappedComputable>, dependency: &[KeyType]);
    fn get_status(&self, key: KeyType) -> Option<Status>;
}

#[derive(Debug, Clone, Copy)]
pub enum Status {
    DepNotFulfilled,
    InQueue,
    Running,
    Completed,
}

struct NodeInfo<K, V> {
    value: V,
    ascendants: Vec<K>,
    refcount: usize,
    status: Status,
}

impl<K, V> NodeInfo<K, V> {
    pub fn new(value: V, ascendants: Vec<K>) -> Self {
        Self {
            value,
            ascendants,
            refcount: 0,
            status: Status::DepNotFulfilled,
        }
    }
}

pub struct LocalScheduler(HashMap<KeyType, NodeInfo<KeyType, Box<dyn WrappedComputable>>>);

impl LocalScheduler {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
}

impl Scheduler for LocalScheduler {
    fn submit(&mut self, task: Box<dyn WrappedComputable>, dependency: &[KeyType]) {
        let uri = task.get_uri();
        if self.0.contains_key(&uri) {
            return;
        }
        for &dep_key in dependency.iter() {
            if !self.0.contains_key(&dep_key) {
                panic!("Dependency not submitted");
            }
        }
        let node_info = NodeInfo::new(task, dependency.iter().copied().collect::<Vec<_>>());
        self.0.insert(uri, node_info);
        for dep_key in dependency.iter() {
            let node_info = self.0.get_mut(dep_key).unwrap();
            node_info.refcount += 1;
        }
    }
    fn get_status(&self, key: KeyType) -> Option<Status> {
        Some(self.0.get(&key)?.status)
    }
}

pub struct Submitter<'a>(&'a dyn Scheduler);
pub struct Receiver<'a>(&'a dyn Scheduler);

pub trait Computable {
    type Output;
    fn get_uri(&self) -> u128;
    fn submit<'a>(&self, submitter: &'a mut Submitter<'a>);
    fn execute<'a>(&self, receiver: &'a mut Receiver<'a>) -> Self::Output;
}

pub trait WrappedComputable {
    fn get_uri(&self) -> u128;
    fn submit<'a>(&self, submitter: &'a mut Submitter<'a>);
    fn execute<'a>(&mut self, receiver: &'a mut Receiver<'a>);
}

pub struct Wrapped<T, C: Computable<Output = T>> {
    core: C,
    result: Arc<RefCell<MaybeUninit<T>>>,
}

impl<T, C: Computable<Output = T>> Wrapped<T, C> {
    pub fn new(core: C) -> Self {
        Self {
            core,
            result: Arc::new(RefCell::new(MaybeUninit::uninit())),
        }
    }
}

impl<T, C: Computable<Output = T>> WrappedComputable for Wrapped<T, C> {
    fn get_uri(&self) -> u128 {
        self.core.get_uri()
    }
    fn submit<'a>(&self, submitter: &'a mut Submitter<'a>) {
        self.core.submit(submitter);
    }
    fn execute<'a>(&mut self, receiver: &'a mut Receiver<'a>) {
        let result = self.core.execute(receiver);
        self.result.borrow_mut().write(result);
    }
}

impl<'a> Submitter<'a> {
    pub fn submit<T>(&mut self, task: impl Computable<Output = T>) {
        todo!()
    }
}

impl<'a> Receiver<'a> {
    pub fn get<T>(&mut self, task: &dyn Computable<Output = T>) -> T {
        todo!()
    }
}
