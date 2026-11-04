// src/test_hooks.rs
//
// Points in the code where a unit test can run code of its own, on the
// thread that gets there, once: to put another thread's write between two
// steps that a race would put it between. Compiled into unit tests only.

use std::cell::RefCell;

type Hook = (&'static str, Box<dyn FnOnce()>);

thread_local! {
    static HOOKS: RefCell<Vec<Hook>> = const { RefCell::new(Vec::new()) };
}

/// Run `run` the next time this thread gets to the point named `point`.
pub fn set(point: &'static str, run: impl FnOnce() + 'static) {
    HOOKS.with(|hooks| hooks.borrow_mut().push((point, Box::new(run))));
}

/// Forget what was set at `point` on this thread and has not run.
pub fn clear(point: &'static str) {
    HOOKS.with(|hooks| hooks.borrow_mut().retain(|(p, _)| *p != point));
}

/// This thread is at the point named `point`: run what a test set there.
pub fn at(point: &'static str) {
    let run = HOOKS.with(|hooks| {
        let mut hooks = hooks.borrow_mut();
        let found = hooks.iter().position(|(p, _)| *p == point)?;
        Some(hooks.remove(found).1)
    });
    if let Some(run) = run {
        run();
    }
}
