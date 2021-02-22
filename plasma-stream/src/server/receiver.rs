// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use super::{
    errors::ObjectReceiveError, status_codes, utils::map_object_ids, ObjectId, MAX_DATA_SIZE,
    MAX_META_SIZE,
};
use plasma_store::{ObjectBuffer, PlasmaClient};
use std::{
    collections::HashSet,
    convert::TryInto,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{io::AsyncReadExt, net::TcpStream};
use tracing::{debug, info};

// OBJECT RECEIVER
// ================================================================================================

pub struct ObjectReceiver {
    /// Address of the peer from which the objects will be received.
    pub peer_addr: SocketAddr,

    /// IDs for object to be received by this receiver.
    pub object_ids: Vec<ObjectId>,

    /// Reference to the plasma store client.
    pub plasma_client: Arc<PlasmaClient>,

    /// Reference to a set of objects currently being received across all receivers.
    pub receiving: Arc<Mutex<HashSet<ObjectId>>>,
}

impl ObjectReceiver {
    /// Prepares this receiver for receiving objects from the specified peer.
    ///
    /// Will return an error if:
    /// * Some of the objects are currently being received as a part of a different request.
    /// * Some of the objects are already present in the local plasma store.
    pub fn prepare(&self) -> Result<(), ObjectReceiveError> {
        // mark the objects as being received; if any of the object IDs is already marked
        // as being received, this will return an error; this is to make sure we don't try
        // to receive the same object twice (e.g. from two different peers)
        self.add_to_receiving()?;

        // make sure the objects are not already in the store
        let plasma_object_ids = map_object_ids(&self.object_ids);
        let in_store = self
            .plasma_client
            .contains_many(&plasma_object_ids)
            .map_err(|err| ObjectReceiveError::StoreError(self.peer_addr, err))?;
        if !in_store.is_empty() {
            let in_store = in_store
                .into_iter()
                .map(|oid| oid.to_bytes().try_into().unwrap())
                .collect();
            return Err(ObjectReceiveError::AlreadyInStore(self.peer_addr, in_store));
        }
        Ok(())
    }

    /// Reads objects from the specified socket and saves them into the local plasma store;
    /// the objects are assumed to be order in the order specified by `object_ids` list.
    ///
    /// Will return an error if:
    /// * The peer sends an error code as the first byte of the response.
    /// * Creating and sealing an object in the local plasma store fails for any reason.
    /// * Peer closes connection for any reason.
    pub async fn run(&self, socket: &mut TcpStream) -> Result<(), ObjectReceiveError> {
        // save peer address for reporting/debugging purposes
        let peer_address = socket
            .peer_addr()
            .map_err(|err| ObjectReceiveError::ConnectionError(None, err))?;
        let num_objects = self.object_ids.len();
        info!("receiving {} objects from {}", num_objects, peer_address);

        // read the first byte of the response; BEGIN indicates the the peer is about to start
        // sending objects; otherwise, there was some kind of error on the peer side an nothing
        // will be sent
        let status = socket
            .read_u8()
            .await
            .map_err(|err| ObjectReceiveError::ConnectionError(Some(peer_address), err))?;
        if status != status_codes::BEGIN {
            return Err(ObjectReceiveError::PeerError(peer_address, status));
        }

        // receive objects one-by-one, and save them to the local plasma store.
        let plasma_object_ids = map_object_ids(&self.object_ids);
        let mut bytes_received = 0;
        for (i, oid) in plasma_object_ids.iter().enumerate() {
            match receive_object(&self.plasma_client, oid, socket, peer_address).await {
                Ok(ob) => {
                    debug!("received object {} from {}", ob, peer_address);
                    bytes_received += ob.size();
                }
                Err(err) => {
                    // try to return to pre-request state by deleting already received objects;
                    // if the delete fails, just swallow the error
                    let _ = self
                        .plasma_client
                        .delete_many(&plasma_object_ids[..(i + 1)]);
                    return Err(err);
                }
            };
        }

        // all objects have been received - so, remove them from the receiving set
        info!(
            "received {} objects ({} bytes) from {}",
            num_objects, bytes_received, peer_address
        );
        Ok(())
    }

    // HELPER METHODS
    // --------------------------------------------------------------------------------------------

    /// Adds all IDs from `object_ids` into the set of objects which are currently being received;
    /// if any of the IDs is already in the list, this will return an error.
    fn add_to_receiving(&self) -> Result<(), ObjectReceiveError> {
        // ensure thread-safety by acquiring a lock to the set of objects being received;
        // `unwrap()` is OK here because no thread will panic wile holding the lock.
        let mut receiving = self.receiving.lock().unwrap();

        // if any of the object IDs is already in the store, return an error
        let mut duplicates = Vec::new();
        for oid in self.object_ids.iter() {
            if receiving.contains(oid) {
                duplicates.push(*oid);
            }
        }

        if !duplicates.is_empty() {
            return Err(ObjectReceiveError::AlreadyReceiving(
                self.peer_addr,
                duplicates,
            ));
        }

        // add all object IDs to the set and return
        receiving.extend(self.object_ids.iter());
        Ok(())
    }
}

impl Drop for ObjectReceiver {
    /// When the receiver is dropped, we need to remove all receiver objects from the receiving set.
    /// We do it here because the receiving set needs to be cleared regardless of whether there were
    /// errors or not.
    fn drop(&mut self) {
        // ensure thread-safety by acquiring a lock to the set of objects being received;
        // `unwrap()` is OK here because no thread will panic wile holding the lock.s
        let mut receiving = self.receiving.lock().unwrap();

        // remove all specified objects form the set
        for oid in self.object_ids.iter() {
            receiving.remove(oid);
        }
    }
}

// HELPER FUNCTIONS
// ================================================================================================

/// Reads a single object from the socket and saves it under the specified 'oid'
/// into the local plasma store.
#[allow(clippy::needless_lifetimes)]
async fn receive_object<'a>(
    pc: &'a PlasmaClient,
    oid: &plasma_store::ObjectId,
    socket: &mut TcpStream,
    from_peer: SocketAddr,
) -> Result<ObjectBuffer<'a>, ObjectReceiveError> {
    // read the header to determine size of object data and metadata
    let (meta_size, data_size) = read_object_header(socket, from_peer).await?;

    // make sure data size is not zero
    if data_size == 0 {
        let oid = oid.to_bytes().try_into().unwrap();
        return Err(ObjectReceiveError::ZeroLengthObjectData(from_peer, oid));
    }

    // make sure data size does not exceed the allowed limit
    if data_size as u64 > MAX_DATA_SIZE {
        let oid = oid.to_bytes().try_into().unwrap();
        return Err(ObjectReceiveError::ObjectDataTooLarge(
            from_peer, oid, data_size,
        ));
    }

    // make sure data size does not exceed the allowed limit
    if meta_size as u64 > MAX_META_SIZE {
        let oid = oid.to_bytes().try_into().unwrap();
        return Err(ObjectReceiveError::ObjectMetaTooLarge(
            from_peer, oid, meta_size,
        ));
    }

    // read the metadata from the socket and save it into a vector
    let mut meta_buf = vec![0u8; meta_size];
    socket
        .read_exact(&mut meta_buf)
        .await
        .map_err(|err| ObjectReceiveError::ConnectionError(Some(from_peer), err))?;

    // create object in the plasma store
    let mut ob = pc
        .create(oid.clone(), data_size, &meta_buf)
        .map_err(|err| ObjectReceiveError::StoreError(from_peer, err))?;

    // read object data from the socket and save it into the object buffer
    let data_buf = ob.data_mut();
    socket
        .read_exact(data_buf)
        .await
        .map_err(|err| ObjectReceiveError::ConnectionError(Some(from_peer), err))?;

    // seal the object to make it available to other clients
    ob.seal()
        .map_err(|err| ObjectReceiveError::StoreError(from_peer, err))?;

    Ok(ob)
}

/// Breaks object header into metadata size (lower 16 bits) and data size (upper 48 bits).
async fn read_object_header(
    socket: &mut TcpStream,
    from_peer: SocketAddr,
) -> Result<(usize, usize), ObjectReceiveError> {
    let header = socket
        .read_u64_le()
        .await
        .map_err(|err| ObjectReceiveError::ConnectionError(Some(from_peer), err))?;
    let meta_size = (header as u16) as usize;
    let data_size = (header >> 16) as usize;
    Ok((meta_size, data_size))
}
