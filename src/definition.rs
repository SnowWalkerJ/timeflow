use super::clock::Clock;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct SnapId(pub u32);
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct Timezone(pub u32);
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct Snap {
    pub timezone: Timezone,
    pub snap: SnapId,
    pub timestamp: u64,
}

pub type ResultType = f32;

pub struct SnapEvent {
    pub snap: Snap,
    pub data: ResultType,
    pub source: i32,
}

pub struct Subscription {
    pub clock: Box<dyn Clock>,
    pub callback: Box<dyn Fn() -> ResultType>,
    pub dependency: Vec<Box<dyn Node>>,
}

pub trait Node {
    fn subscribe(&self) -> Vec<Subscription>;
}
