// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use crate::{
    errors::RequestError, ObjectId, MAX_NUM_SYNC_PEERS, MAX_OBJECT_ID_LIST_LEN, OBJECT_ID_BYTES,
};
use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

// CONSTANTS
// ================================================================================================
const SYNC_TYPE_ID: u8 = 1;
const COPY_TYPE_ID: u8 = 2;
const TAKE_TYPE_ID: u8 = 3;

const IPV4_TYPE_ID: u8 = 4;
const IPV6_TYPE_ID: u8 = 6;

// REQUEST
// ================================================================================================

#[derive(Debug)]
pub enum Request {
    Sync(Vec<PeerRequest>),
    Copy(Vec<ObjectId>),
    Take(Vec<ObjectId>),
}

impl Request {
    /// Reads a request from the specified socket. This function will return when:
    /// * A well-formed request has been read.
    /// * The socket has been closed; in this case `None` will be returned.
    /// * The data read from the socket does not represent a valid request; in this case
    ///   an error will be returned.
    pub async fn read_from(socket: &mut TcpStream) -> crate::Result<Option<Self>> {
        // determine request type; also return `None` if the connection has been closed
        let request_type = match socket.read_u8().await {
            Ok(request_type) => request_type,
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        // based on the type, read the rest of the request
        match request_type {
            SYNC_TYPE_ID => {
                let num_peer_requests = socket.read_u16_le().await?;
                let mut peer_requests = Vec::with_capacity(num_peer_requests as usize);
                for _ in 0..num_peer_requests {
                    let peer_request = PeerRequest::read_from(socket).await?;
                    peer_requests.push(peer_request);
                }
                Ok(Some(Self::Sync(peer_requests)))
            }
            COPY_TYPE_ID => {
                let object_ids = read_object_id_list(socket).await?;
                Ok(Some(Self::Copy(object_ids)))
            }
            TAKE_TYPE_ID => {
                let object_ids = read_object_id_list(socket).await?;
                Ok(Some(Self::Take(object_ids)))
            }
            _ => Err(RequestError::InvalidRequestType(request_type).into()),
        }
    }

    /// Writes this request into the socket.
    pub async fn write_into(&self, socket: &mut TcpStream) -> Result<(), std::io::Error> {
        match self {
            Request::Sync(peer_requests) => {
                socket.write_u8(SYNC_TYPE_ID).await?;
                socket.write_u16_le(peer_requests.len() as u16).await?;
                for peer_request in peer_requests.iter() {
                    peer_request.write_into(socket).await?;
                }
            }
            Request::Copy(object_ids) => {
                socket.write_u8(COPY_TYPE_ID).await?;
                write_object_id_list(object_ids, socket).await?;
            }
            Request::Take(object_ids) => {
                socket.write_u8(TAKE_TYPE_ID).await?;
                write_object_id_list(object_ids, socket).await?;
            }
        }
        Ok(())
    }

    /// Checks if this request is valid. Specifically, makes sure:
    /// * There are no duplicated object IDs present in the request.
    /// * Number of objects in a single request does not exceed the allowed limit.
    pub fn validate(&self) -> Result<(), RequestError> {
        match self {
            Request::Sync(peer_requests) => {
                // make sure peer request lists is neither too long nor too short
                if peer_requests.is_empty() {
                    return Err(RequestError::PeerRequestListTooShort);
                }
                if peer_requests.len() > MAX_NUM_SYNC_PEERS {
                    return Err(RequestError::PeerRequestListTooLong(peer_requests.len()));
                }
                // TODO: use non-cryptographic hashing
                let mut unique_objects = HashSet::new();
                for peer_request in peer_requests.iter() {
                    peer_request.validate()?;
                    let incoming_objects = peer_request.incoming_objects();
                    // if a duplicate ID is found, return an error
                    for oid in incoming_objects {
                        if !unique_objects.insert(oid) {
                            return Err(RequestError::DuplicateObjectIds);
                        }
                    }
                }
            }
            Request::Take(object_ids) | Request::Copy(object_ids) => {
                // make sure object ID list is neither too long nor too short
                if object_ids.is_empty() {
                    return Err(RequestError::ObjectIdListTooShort);
                }
                if object_ids.len() > MAX_OBJECT_ID_LIST_LEN {
                    return Err(RequestError::ObjectIdListTooLong(object_ids.len()));
                }
                // if a duplicate ID is found, return an error
                // TODO: use non-cryptographic hashing
                let mut unique_objects = HashSet::new();
                for oid in object_ids {
                    if !unique_objects.insert(oid) {
                        return Err(RequestError::DuplicateObjectIds);
                    }
                }
            }
        }
        Ok(())
    }
}

impl Display for Request {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        match self {
            Request::Sync(requests) => {
                write!(f, "SYNC")?;
                for request in requests.iter() {
                    write!(f, "\n{}", request)?;
                }
                write!(f, "")
            }
            Request::Copy(object_ids) => {
                write!(
                    f,
                    "COPY {:?}",
                    object_ids.iter().map(hex::encode).collect::<Vec<_>>()
                )
            }
            Request::Take(object_ids) => {
                write!(
                    f,
                    "TAKE {:?}",
                    object_ids.iter().map(hex::encode).collect::<Vec<_>>()
                )
            }
        }
    }
}

// PEER REQUESTS
// ================================================================================================

#[derive(Debug)]
pub enum PeerRequest {
    Copy {
        from: SocketAddr,
        objects: Vec<ObjectId>,
    },
    Take {
        from: SocketAddr,
        objects: Vec<ObjectId>,
    },
}

impl PeerRequest {
    /// Reads a SYNC peer request from the specified socket.
    pub async fn read_from(socket: &mut TcpStream) -> crate::Result<Self> {
        let request_type = socket.read_u8().await?;
        match request_type {
            COPY_TYPE_ID => {
                let from = read_socket_addr(socket).await?;
                let objects = read_object_id_list(socket).await?;
                Ok(PeerRequest::Copy { from, objects })
            }
            TAKE_TYPE_ID => {
                let from = read_socket_addr(socket).await?;
                let objects = read_object_id_list(socket).await?;
                Ok(PeerRequest::Take { from, objects })
            }
            _ => Err(RequestError::InvalidPeerRequestType(request_type).into()),
        }
    }

    // Writes a SYNC peer request into the specified socket.
    pub async fn write_into(&self, socket: &mut TcpStream) -> Result<(), std::io::Error> {
        match self {
            Self::Copy { from, objects } => {
                socket.write_u8(COPY_TYPE_ID).await?;
                write_peer_addr(from, socket).await?;
                write_object_id_list(objects, socket).await?;
            }
            Self::Take { from, objects } => {
                socket.write_u8(TAKE_TYPE_ID).await?;
                write_peer_addr(from, socket).await?;
                write_object_id_list(objects, socket).await?;
            }
        }
        Ok(())
    }

    // Checks whether this peer request is valid.
    pub fn validate(&self) -> Result<(), RequestError> {
        match self {
            Self::Copy { objects, .. } | Self::Take { objects, .. } => {
                // make sure object ID list is neither too long nor too short
                if objects.is_empty() {
                    return Err(RequestError::ObjectIdListTooShort);
                }
                if objects.len() > MAX_OBJECT_ID_LIST_LEN {
                    return Err(RequestError::ObjectIdListTooLong(objects.len()));
                }
            }
        }
        Ok(())
    }

    /// Gets a list of object IDs which will be received upon execution of this SYNC peer request.
    pub fn incoming_objects(&self) -> &[ObjectId] {
        match self {
            PeerRequest::Copy { objects, .. } => &objects,
            PeerRequest::Take { objects, .. } => &objects,
        }
    }

    /// Returns true if this peer requests contains the specified peer address.
    pub fn contains_peer(&self, address: &SocketAddr) -> bool {
        match self {
            PeerRequest::Copy { from, .. } => from == address,
            PeerRequest::Take { from, .. } => from == address,
        }
    }
}

impl Display for PeerRequest {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        match self {
            PeerRequest::Copy { from, objects } => {
                write!(
                    f,
                    "COPY {} {:?}",
                    from,
                    objects.iter().map(hex::encode).collect::<Vec<_>>()
                )
            }
            PeerRequest::Take { from, objects } => {
                write!(
                    f,
                    "TAKE {} {:x?}",
                    from,
                    objects.iter().map(hex::encode).collect::<Vec<_>>()
                )
            }
        }
    }
}

// HELPER READERS
// ================================================================================================

/// Reads peer address from the specified socket.
async fn read_socket_addr(socket: &mut TcpStream) -> crate::Result<SocketAddr> {
    let addr_type = socket.read_u8().await?;
    let port = socket.read_u16_le().await?;

    match addr_type {
        IPV4_TYPE_ID => {
            let addr = read_ipv4_address(socket).await?;
            Ok(SocketAddr::new(IpAddr::V4(addr), port))
        }
        IPV6_TYPE_ID => {
            let addr = read_ipv6_address(socket).await?;
            Ok(SocketAddr::new(IpAddr::V6(addr), port))
        }
        _ => Err(RequestError::InvalidPeerAddressType(addr_type).into()),
    }
}

/// Reads an IPv4 address from the specified socket.
async fn read_ipv4_address(socket: &mut TcpStream) -> Result<Ipv4Addr, std::io::Error> {
    let a = socket.read_u32_le().await?;
    Ok(Ipv4Addr::new(
        a as u8,
        (a >> 8) as u8,
        (a >> 16) as u8,
        (a >> 24) as u8,
    ))
}

/// Reads an IPv6 address from the specified socket.
async fn read_ipv6_address(_socket: &mut TcpStream) -> Result<Ipv6Addr, std::io::Error> {
    // TODO: add support for IPv6 addresses
    unimplemented!()
}

/// Reads a list of object IDs from the specified socket.
async fn read_object_id_list(socket: &mut TcpStream) -> Result<Vec<ObjectId>, std::io::Error> {
    // determine number of object IDs
    let num_ids = socket.read_u16_le().await? as usize;

    // read all object ID bytes
    let mut result = vec![0u8; OBJECT_ID_BYTES * num_ids];
    socket.read_exact(&mut result).await?;

    // convert the vector of bytes into a vector of 20-byte arrays
    let mut v = std::mem::ManuallyDrop::new(result);
    let p = v.as_mut_ptr();
    let len = v.len() / OBJECT_ID_BYTES;
    let cap = v.capacity() / OBJECT_ID_BYTES;
    unsafe { Ok(Vec::from_raw_parts(p as *mut ObjectId, len, cap)) }
}

// HELPER WRITERS
// ================================================================================================

/// Writes a list of object IDs into the socket. Number of object IDs is written into the
/// socket first (as u16), followed by the actual object IDs.
async fn write_object_id_list(
    object_ids: &[ObjectId],
    socket: &mut TcpStream,
) -> Result<(), std::io::Error> {
    socket.write_u16_le(object_ids.len() as u16).await?;
    for id in object_ids.iter() {
        socket.write_all(id).await?;
    }
    Ok(())
}

/// Writes socket address of the peer into the socket.
async fn write_peer_addr(
    peer_addr: &SocketAddr,
    socket: &mut TcpStream,
) -> Result<(), std::io::Error> {
    match peer_addr {
        SocketAddr::V4(peer_addr) => {
            socket.write_u8(IPV4_TYPE_ID).await?;
            socket.write_u16_le(peer_addr.port()).await?;
            socket.write_all(&peer_addr.ip().octets()).await?;
        }
        SocketAddr::V6(peer_addr) => {
            socket.write_u8(IPV6_TYPE_ID).await?;
            socket.write_u16_le(peer_addr.port()).await?;
            socket.write_all(&peer_addr.ip().octets()).await?;
        }
    }
    Ok(())
}
