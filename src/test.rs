use fractal::{CompleteTask, RawTaskRef, SubScheduler, Task};
use fractal_macro::task_op;

#[task_op]
fn add(a: &f32, b: &f32) -> f32 {
    a + b
}

#[task_op]
fn one() -> f32 {
    1.0
}

#[test]
fn test() {
    use std::time::Duration;

    use fractal::{WrappedTask, run, submit};
    run(|mut scheduler| {
        let a = submit!(scheduler, one()).unwrap();
        let b = submit!(scheduler, one()).unwrap();
        let f = submit!(scheduler, add(a, b)).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let tmp = scheduler.get(&f);
        let result: f32 = *tmp.unwrap().downcast_ref().unwrap();
        println!("{}", result);
    });
}
