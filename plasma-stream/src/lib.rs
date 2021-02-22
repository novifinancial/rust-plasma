// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

mod request;
pub use request::{PeerRequest, Request};

mod client;
pub use client::Client;

pub mod errors;
pub mod utils;

// CONSTANTS
// ================================================================================================

pub const OBJECT_ID_BYTES: usize = 20;

pub const MAX_META_SIZE: u64 = 65_536; // 2^16 or 64 KB
pub const MAX_DATA_SIZE: u64 = 17_592_186_044_416; // 2^44 or 16 TB

const MAX_OBJECT_ID_LIST_LEN: usize = 65_536; // 2^16
const MAX_NUM_SYNC_PEERS: usize = 1024;

pub mod status_codes {
    pub const BEGIN: u8 = 0x00;
    pub const SUCCESS: u8 = 0x41;
    pub const OB_META_TOO_LARGE_ERR: u8 = 0x50;
    pub const OB_DATA_TOO_LARGE_ERR: u8 = 0x51;
    pub const OB_DATA_ZERO_LENGTH_ERR: u8 = 0x52;
    pub const PLASMA_STORE_ERR: u8 = 0x60;
    pub const PEER_PLASMA_STORE_ERR: u8 = 0x61;
    pub const PEER_REQUEST_PANICKED: u8 = 0x62;
    pub const OB_DELETION_SCHEDULED_ERR: u8 = 0x70;
    pub const OB_NOT_FOUND_ERR: u8 = 0x71;
    pub const OB_ALREADY_RECEIVING_ERR: u8 = 0x80;
    pub const OB_ALREADY_IN_STORE_ERR: u8 = 0x81;
    pub const PEER_CONNECTION_ERR: u8 = 0x90;
    pub const CLIENT_CONNECTION_ERR: u8 = 0x91;
}

// CONVENIENCE TYPES
// ================================================================================================

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;

pub type ObjectId = [u8; OBJECT_ID_BYTES];
