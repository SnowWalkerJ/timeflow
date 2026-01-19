use super::definition::{Snap, Timezone};

pub trait Clock {
    fn timezone(&self) -> Timezone;
    fn tick(&self, snap: &Snap) -> bool;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Base {
    timezone: Timezone,
}

impl Clock for Base {
    fn tick(&self, snap: &Snap) -> bool {
        snap.timezone == self.timezone
    }
    fn timezone(&self) -> Timezone {
        self.timezone
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Freq<Base: Clock> {
    base: Base,
    freq: u32,
    offset: u32,
}

impl<Base: Clock> Freq<Base> {
    pub fn new(base: Base, freq: u32, offset: u32) -> Self {
        Self { base, freq, offset }
    }
}

impl<Base: Clock> Clock for Freq<Base> {
    fn tick(&self, snap: &Snap) -> bool {
        self.base.tick(snap) && (snap.snap.0 - self.offset) % self.freq == 0
    }
    fn timezone(&self) -> Timezone {
        self.base.timezone()
    }
}
