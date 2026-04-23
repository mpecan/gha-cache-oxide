//! Test helper for serialising env-var mutations across config tests.
//!
//! The process environment is shared mutable state; Cargo's test harness runs
//! tests in parallel by default. Without serialisation, parallel tests can
//! read each other's env mutations and flake. This helper snapshots the
//! requested vars, mutates them, runs the closure, and restores the original
//! state — all under a single global `Mutex`.

use std::sync::Mutex;

/// Global gate. Every test that mutates env vars must acquire this first.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Mutates `setup` so that `var` is overridden to `val`. Looks up by name
/// so tests don't care about the underlying slice ordering. If the var is
/// not already in the slice, appends it.
pub fn set(
    setup: &mut Vec<(&'static str, Option<&'static str>)>,
    var: &'static str,
    val: Option<&'static str>,
) {
    for entry in setup.iter_mut() {
        if entry.0 == var {
            entry.1 = val;
            return;
        }
    }
    setup.push((var, val));
}

/// Runs `f` with the given env-var mutations applied, then restores the
/// original values. `None` means "remove this var"; `Some(val)` sets it.
pub fn with_env<F: FnOnce()>(setup: &[(&str, Option<&str>)], f: F) {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let saved: Vec<(String, Option<String>)> = setup
        .iter()
        .map(|(k, _)| ((*k).to_string(), std::env::var(*k).ok()))
        .collect();
    for (k, v) in setup {
        match v {
            Some(val) => unsafe { std::env::set_var(k, val) },
            None => unsafe { std::env::remove_var(k) },
        }
    }
    f();
    for (k, v) in saved {
        match v {
            Some(val) => unsafe { std::env::set_var(&k, val) },
            None => unsafe { std::env::remove_var(&k) },
        }
    }
}
