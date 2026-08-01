//! Optional Layer-2 components (gated behind the `l2` feature).

pub mod frame;
pub mod link;

#[cfg(feature = "l2_pfc")]
pub mod pfc;
