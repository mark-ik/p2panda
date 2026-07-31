// SPDX-License-Identifier: MIT OR Apache-2.0

//! Eventually consistent, local-first sync protocol based on append-only logs.
mod api;
mod builder;
#[cfg(test)]
mod tests;

pub use api::{LogSync, LogSyncError};
pub use builder::Builder;

/// Default sync protocol identifier (ALPN), used when a builder does not name
/// its own.
///
/// The endpoint routes incoming connections by protocol id and keeps exactly
/// one handler per id, so two `LogSync` instances sharing one endpoint MUST
/// name distinct ids via [`Builder::protocol_id`] — otherwise the
/// second registration silently replaces the first and receives all of its
/// inbound sync sessions.
pub const LOG_SYNC_PROTOCOL_ID: &[u8] = b"p2panda/log_sync/v1";
