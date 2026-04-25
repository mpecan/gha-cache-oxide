//! Background tasks that run on a periodic schedule.
//!
//! Currently houses the [`cleanup`] subsystem (issue #18) — five
//! cleanup jobs ported from upstream `tasks/cleanup/*.ts` plus the
//! single hourly scheduler that drives them.

pub mod cleanup;
