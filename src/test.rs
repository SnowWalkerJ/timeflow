use fractal::{CompleteTask, RawTaskRef, SubScheduler, Task};
use fractal_macro::task_op;

#[task_op]
fn func(a: &f32) -> f32 {
    a + 1.0
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
        let f = submit!(scheduler, func(a)).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let result: f32 = *scheduler
            .get(&f)
            .unwrap()
            .read()
            .unwrap()
            .downcast_ref()
            .unwrap();
        println!("{}", result);
    });
}
