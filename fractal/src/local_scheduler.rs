use super::scheduler::*;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::rc::Rc;

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
        let node_info = NodeInfo::new(task, dependency.iter().copied().collect::<HashSet<_>>());
        self.0.insert(uri, node_info);
        for dep_key in dependency.iter() {
            let dep_node_info = self.0.get_mut(dep_key).unwrap();
            dep_node_info.descendants.insert(uri);
        }
    }
    fn get_result(&self, key: KeyType) -> Option<&dyn std::any::Any> {
        let v = self.0.get(&key)?;
        let result = &v.result;
        if let Some(r) = result {
            Some(r.deref())
        } else {
            None
        }
    }
    fn run(&mut self) {
        let mut queue = self
            .0
            .iter()
            .filter_map(|(&key, node_info)| {
                if node_info.ascendants.is_empty() {
                    Some(key)
                } else {
                    None
                }
            })
            .collect::<std::collections::VecDeque<KeyType>>();

        while !queue.is_empty() {
            if let Some(key) = queue.pop_front() {
                {
                    let node_info = self.0.get_mut(&key).unwrap();
                    node_info.status = Status::Running;
                }
                let node_info = self.0.get(&key).unwrap();
                let receiver = Receiver::new(self);
                let result = node_info.value.execute(&receiver);
                {
                    let node_info = self.0.get_mut(&key).unwrap();
                    node_info.status = Status::Completed;
                    node_info.result = Some(result);
                }
                let node_info = self.0.get(&key).unwrap();
                let descendants = node_info.descendants.clone();
                for descendant_uri in descendants.iter() {
                    let dep_info = self.0.get_mut(descendant_uri).unwrap();
                    dep_info.ascendants.remove(&key);
                    if dep_info.ascendants.is_empty() {
                        dep_info.status = Status::InQueue;
                        queue.push_back(*descendant_uri);
                    }
                }
            }
        }
    }
}

struct NodeInfo<K, V> {
    value: V,
    ascendants: HashSet<K>,
    descendants: HashSet<K>,
    status: Status,
    result: Option<Box<dyn Any>>,
}

impl<K, V> NodeInfo<K, V> {
    pub fn new(value: V, ascendants: HashSet<K>) -> Self {
        Self {
            value,
            ascendants,
            descendants: HashSet::new(),
            status: Status::DepNotFulfilled,
            result: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_local_scheduler() {
        struct Const(pub f32);
        impl Computable for Const {
            type Output = f32;
            fn get_uri(&self) -> u128 {
                (self.0 * 1000000000f32) as u128
            }
            fn submit<'a, 'b: 'a>(&self, _submitter: &'a mut Submitter<'b>) -> HashSet<KeyType> {
                HashSet::new()
            }
            fn execute(&self, _receiver: &Receiver) -> Box<Self::Output> {
                Box::new(self.0)
            }
        }
        struct Add {
            pub lhs: Rc<dyn Computable<Output = f32>>,
            pub rhs: Rc<dyn Computable<Output = f32>>,
        }
        impl Add {
            fn new(
                lhs: Rc<dyn Computable<Output = f32>>,
                rhs: Rc<dyn Computable<Output = f32>>,
            ) -> Self {
                Self { lhs, rhs }
            }
        }
        impl Computable for Add {
            type Output = f32;
            fn get_uri(&self) -> u128 {
                self.lhs.get_uri() + self.rhs.get_uri()
            }
            fn submit<'a, 'b: 'a>(&self, submitter: &'a mut Submitter<'b>) -> HashSet<KeyType> {
                submitter.submit(Rc::clone(&self.lhs));
                submitter.submit(Rc::clone(&self.rhs));
                let mut result = HashSet::with_capacity(2);
                result.insert(self.lhs.get_uri());
                result.insert(self.rhs.get_uri());
                result
            }
            fn execute(&self, receiver: &Receiver) -> Box<Self::Output> {
                let lhs = receiver.get_result::<f32>(self.lhs.get_uri()).unwrap();
                let rhs = receiver.get_result::<f32>(self.rhs.get_uri()).unwrap();
                Box::new(lhs + rhs)
            }
        }
        let lhs = Const(1.0);
        let rhs = Const(2.3);
        let output = Add::new(Rc::new(lhs), Rc::new(rhs));
        let out_uri = output.get_uri();
        let mut scheduler = LocalScheduler::new();
        let mut submitter = Submitter::new(&mut scheduler);
        submitter.submit(Rc::new(output));
        println!("{}", scheduler.0.len());
        scheduler.run();
        let receiver = Receiver::new(&scheduler);
        println!("{}", receiver.get_result::<f32>(out_uri).unwrap());
    }
}
