// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use crate::{status_codes, ObjectId, MAX_DATA_SIZE, MAX_META_SIZE};
use plasma_store::PlasmaError;
use std::{
    fmt::{self, Display, Formatter},
    net::SocketAddr,
};
use thiserror::{private::AsDynError, Error};
use tokio::task::JoinError;

// OBJECT SEND ERROR
// ================================================================================================

/// Describes possible errors which can be encountered while sending objects from one Plasma
/// Stream server to another.
#[derive(Debug)]
pub enum ObjectSendError {
    ObjectDeletionScheduled(SocketAddr, Vec<ObjectId>),
    ObjectMetaTooLarge(SocketAddr, ObjectId, usize),
    ObjectDataTooLarge(SocketAddr, ObjectId, usize),
    StoreError(SocketAddr, PlasmaError),
    ObjectsNotFound(SocketAddr, Vec<ObjectId>),
    ConnectionError(Option<SocketAddr>, std::io::Error),
}

impl ObjectSendError {
    pub fn response_code(&self) -> Option<u8> {
        match self {
            Self::ObjectDeletionScheduled(_, _) => Some(status_codes::OB_DELETION_SCHEDULED_ERR),
            Self::ObjectMetaTooLarge(_, _, _) => Some(status_codes::OB_META_TOO_LARGE_ERR),
            Self::ObjectDataTooLarge(_, _, _) => Some(status_codes::OB_DATA_TOO_LARGE_ERR),
            Self::ObjectsNotFound(_, _) => Some(status_codes::OB_NOT_FOUND_ERR),
            Self::StoreError(_, _) => Some(status_codes::PLASMA_STORE_ERR),
            Self::ConnectionError(_, _) => None,
        }
    }
}

impl Display for ObjectSendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ObjectDeletionScheduled(peer, objects) => {
                write!(
                    f,
                    "failed to send objects to {}; object scheduled for deletion:",
                    peer
                )?;
                for oid in objects {
                    write!(f, "\n0x{}", hex::encode(oid))?
                }
            }
            Self::ObjectMetaTooLarge(peer, oid, _) => {
                write!(
                    f,
                    "failed to send objects to {}; metadata too large for 0x{}",
                    peer,
                    hex::encode(oid),
                )?;
            }
            Self::ObjectDataTooLarge(peer, oid, _) => {
                write!(
                    f,
                    "failed to send objects to {}; data too large for 0x{}",
                    peer,
                    hex::encode(oid),
                )?;
            }
            Self::ObjectsNotFound(peer, objects) => {
                write!(f, "failed to send objects to {}; objects not found:", peer)?;
                for oid in objects {
                    write!(f, "\n0x{}", hex::encode(oid))?
                }
            }
            Self::StoreError(peer, err) => {
                write!(
                    f,
                    "failed to send objects to {}; plasma store error: {}",
                    peer, err,
                )?;
            }
            Self::ConnectionError(peer, err) => match peer {
                Some(peer) => write!(f, "failed to send objects to {}: {}", peer, err)?,
                None => write!(f, "failed to send objects: {}", err)?,
            },
        };

        Ok(())
    }
}

impl std::error::Error for ObjectSendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ConnectionError(_, err) => Some(err.as_dyn_error()),
            Self::StoreError(_, err) => Some(err.as_dyn_error()),
            _ => None,
        }
    }
}

// OBJECT RECEIVE ERROR
// ================================================================================================

/// Describes possible errors which can be encountered while receiving objects sent from one
/// Plasma stream server to another.
#[derive(Debug)]
pub enum ObjectReceiveError {
    AlreadyReceiving(SocketAddr, Vec<ObjectId>),
    AlreadyInStore(SocketAddr, Vec<ObjectId>),
    ObjectMetaTooLarge(SocketAddr, ObjectId, usize),
    ObjectDataTooLarge(SocketAddr, ObjectId, usize),
    ZeroLengthObjectData(SocketAddr, ObjectId),
    PeerError(SocketAddr, u8),
    StoreError(SocketAddr, PlasmaError),
    ConnectionError(Option<SocketAddr>, std::io::Error),
}

impl ObjectReceiveError {
    pub fn response_code(&self) -> u8 {
        match self {
            Self::AlreadyReceiving(_, _) => status_codes::OB_ALREADY_RECEIVING_ERR,
            Self::AlreadyInStore(_, _) => status_codes::OB_ALREADY_IN_STORE_ERR,
            Self::ObjectMetaTooLarge(_, _, _) => status_codes::OB_META_TOO_LARGE_ERR,
            Self::ObjectDataTooLarge(_, _, _) => status_codes::OB_DATA_TOO_LARGE_ERR,
            Self::ZeroLengthObjectData(_, _) => status_codes::OB_DATA_ZERO_LENGTH_ERR,
            Self::PeerError(_, status_code) => match *status_code {
                status_codes::PLASMA_STORE_ERR => status_codes::PEER_PLASMA_STORE_ERR,
                _ => *status_code,
            },
            Self::StoreError(_, _) => status_codes::PLASMA_STORE_ERR,
            Self::ConnectionError(_, _) => status_codes::PEER_CONNECTION_ERR,
        }
    }
}

impl Display for ObjectReceiveError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyReceiving(peer, objects) => {
                write!(
                    f,
                    "did not request objects from {}; already receiving objects:",
                    peer
                )?;
                for oid in objects {
                    write!(f, "\n0x{}", hex::encode(oid))?
                }
            }
            Self::AlreadyInStore(peer, objects) => {
                write!(
                    f,
                    "did not request objects from {}; objects already in store:",
                    peer
                )?;
                for oid in objects {
                    write!(f, "\n0x{}", hex::encode(oid))?
                }
            }
            Self::ObjectMetaTooLarge(peer, oid, _) => {
                write!(
                    f,
                    "failed to receive objects from {}; metadata too large for 0x{}",
                    peer,
                    hex::encode(oid),
                )?;
            }
            Self::ObjectDataTooLarge(peer, oid, _) => {
                write!(
                    f,
                    "failed to receiver objects from {}; data too large for 0x{}",
                    peer,
                    hex::encode(oid),
                )?;
            }
            Self::ZeroLengthObjectData(peer, oid) => {
                write!(
                    f,
                    "failed to receiver objects from {}; zero-length data for 0x{}",
                    peer,
                    hex::encode(oid),
                )?;
            }
            Self::PeerError(peer, response_code) => {
                write!(f, "failed to receive objects from {}; ", peer)?;
                match *response_code {
                    status_codes::OB_DELETION_SCHEDULED_ERR => write!(f, "deletion in progress")?,
                    status_codes::OB_META_TOO_LARGE_ERR => write!(f, "object meta too large")?,
                    status_codes::OB_DATA_TOO_LARGE_ERR => write!(f, "object data too large")?,
                    status_codes::OB_NOT_FOUND_ERR => write!(f, "not found")?,
                    status_codes::PLASMA_STORE_ERR => write!(f, "peer plasma store error")?,
                    _ => write!(f, "unknown error code: {}", response_code)?,
                }
            }
            Self::StoreError(peer, err) => {
                write!(
                    f,
                    "failed to receive objects from {}; plasma store error: {}",
                    peer, err,
                )?;
            }
            Self::ConnectionError(peer, err) => match peer {
                Some(peer) => write!(f, "failed to receive objects from {}: {}", peer, err)?,
                None => write!(f, "failed to receive objects: {}", err)?,
            },
        };

        Ok(())
    }
}

impl std::error::Error for ObjectReceiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ConnectionError(_, err) => Some(err.as_dyn_error()),
            Self::StoreError(_, err) => Some(err.as_dyn_error()),
            _ => None,
        }
    }
}

// REQUEST ERROR
// ================================================================================================

/// Describes errors which can be encountered when parsing requests.
#[derive(Error, Debug)]
pub enum RequestError {
    #[error("invalid request type {0}")]
    InvalidRequestType(u8),

    #[error("invalid peer request type {0}")]
    InvalidPeerRequestType(u8),

    #[error("invalid peer address type {0}")]
    InvalidPeerAddressType(u8),

    #[error("object ID list is empty")]
    ObjectIdListTooShort,

    #[error("object ID list is too long {0}")]
    ObjectIdListTooLong(usize),

    #[error("request contains duplicate object IDs")]
    DuplicateObjectIds,

    #[error("peer request list is empty")]
    PeerRequestListTooShort,

    #[error("peer request list is too long {0}")]
    PeerRequestListTooLong(usize),
}

// SYNC ERROR
// ================================================================================================

/// Describes errors which can be encountered while fulfilling SYNC requests.
#[derive(Debug)]
pub enum SyncError {
    PeerConnectionFailed(SocketAddr, std::io::Error),
    PeerRequestNotSent(SocketAddr, std::io::Error),
    ReceiverError(ObjectReceiveError),
    PeerRequestPanicked(JoinError),
    ClientConnectionError(std::io::Error),
    PeerAddressIsSelf,
}

impl SyncError {
    pub fn response_code(&self) -> u8 {
        match self {
            Self::PeerConnectionFailed(_, _) => status_codes::PEER_CONNECTION_ERR,
            Self::PeerRequestNotSent(_, _) => status_codes::PEER_CONNECTION_ERR,
            Self::ReceiverError(err) => err.response_code(),
            Self::PeerRequestPanicked(_) => status_codes::PEER_REQUEST_PANICKED,
            Self::ClientConnectionError(_) => status_codes::CLIENT_CONNECTION_ERR,
            Self::PeerAddressIsSelf => status_codes::PEER_CONNECTION_ERR,
        }
    }
}

impl Display for SyncError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerConnectionFailed(peer, err) => {
                write!(f, "peer connection to {} failed: {}", peer, err)?
            }
            Self::PeerRequestNotSent(peer, err) => {
                write!(f, "failed to send request to {}: {}", peer, err)?
            }
            Self::ReceiverError(err) => write!(f, "f{}", err)?,
            Self::PeerRequestPanicked(err) => write!(f, "peer request panicked: {}", err)?,
            Self::ClientConnectionError(err) => write!(f, "client connection failed: {}", err)?,
            Self::PeerAddressIsSelf => write!(f, "cannot make a peer request to self")?,
        };
        Ok(())
    }
}

impl std::error::Error for SyncError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::PeerConnectionFailed(_, err) => Some(err.as_dyn_error()),
            Self::PeerRequestNotSent(_, err) => Some(err.as_dyn_error()),
            _ => None,
        }
    }
}

// PEER RESULT
// ================================================================================================

/// Describes results which are forwarded to the initiator of a SYNC request.
#[derive(Debug)]
pub enum PeerResult {
    Ok,
    ObjectMetaTooLarge,
    ObjectDataTooLarge,
    ZeroLengthObjectData,
    PlasmaStoreError,
    PeerPlasmaStoreError,
    PeerRequestPanicked,
    ObjectDeletionScheduled,
    ObjectsNotFound,
    AlreadyReceiving,
    AlreadyInStore,
    PeerConnectionError,
    UnknownError,
}

impl PeerResult {
    pub fn from(result: u8) -> Self {
        match result {
            status_codes::SUCCESS => Self::Ok,
            status_codes::OB_META_TOO_LARGE_ERR => Self::ObjectMetaTooLarge,
            status_codes::OB_DATA_TOO_LARGE_ERR => Self::ObjectDataTooLarge,
            status_codes::OB_DATA_ZERO_LENGTH_ERR => Self::ZeroLengthObjectData,
            status_codes::PLASMA_STORE_ERR => Self::PlasmaStoreError,
            status_codes::PEER_PLASMA_STORE_ERR => Self::PeerPlasmaStoreError,
            status_codes::PEER_REQUEST_PANICKED => Self::PeerRequestPanicked,
            status_codes::OB_DELETION_SCHEDULED_ERR => Self::ObjectDeletionScheduled,
            status_codes::OB_NOT_FOUND_ERR => Self::ObjectsNotFound,
            status_codes::OB_ALREADY_RECEIVING_ERR => Self::AlreadyReceiving,
            status_codes::OB_ALREADY_IN_STORE_ERR => Self::AlreadyInStore,
            status_codes::PEER_CONNECTION_ERR => Self::PeerConnectionError,
            _ => Self::UnknownError,
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }
}

impl Display for PeerResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => write!(f, "Ok")?,
            Self::ObjectMetaTooLarge => {
                write!(f, "object metadata exceeds {} bytes", MAX_META_SIZE)?
            }
            Self::ObjectDataTooLarge => write!(f, "object data exceeds {} bytes", MAX_DATA_SIZE)?,
            Self::ZeroLengthObjectData => write!(f, "zero-length object data")?,
            Self::PlasmaStoreError => write!(f, "local plasma store error")?,
            Self::PeerPlasmaStoreError => write!(f, "peer plasma store error")?,
            Self::PeerRequestPanicked => write!(f, "peer request panicked")?,
            Self::ObjectDeletionScheduled => {
                write!(f, "requested object(s) scheduled for deletion")?
            }
            Self::ObjectsNotFound => write!(f, "requested object(s) not found")?,
            Self::AlreadyReceiving => write!(f, "duplicate request for object(s)")?,
            Self::AlreadyInStore => write!(f, "requested object(s) already in local store")?,
            Self::PeerConnectionError => write!(f, "connection to peer(s) failed")?,
            Self::UnknownError => write!(f, "Unknown error")?,
        };
        Ok(())
    }
}

// CLIENT ERROR
// ================================================================================================

#[derive(Debug)]
pub enum ClientError {
    MalformedRequest(RequestError),
    ConnectionError(String, std::io::Error),
    SyncError(Vec<PeerResult>),
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedRequest(err) => write!(f, "malformed request: {}", err)?,
            Self::ConnectionError(msg, err) => write!(f, "{}: {}", msg, err)?,
            Self::SyncError(results) => {
                write!(f, "peer requests resolved as follows:")?;
                for result in results {
                    write!(f, "\n{}", result)?;
                }
            }
        };

        Ok(())
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ConnectionError(_, err) => Some(err.as_dyn_error()),
            Self::MalformedRequest(err) => Some(err.as_dyn_error()),
            _ => None,
        }
    }
}
