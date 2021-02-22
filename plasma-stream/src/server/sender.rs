// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use super::{
    errors::ObjectSendError, status_codes, utils::map_object_ids, ObjectId, MAX_DATA_SIZE,
    MAX_META_SIZE,
};
use plasma_store::{ObjectBuffer, PlasmaClient};
use std::{
    collections::HashSet,
    convert::TryInto,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tracing::{debug, error, info};

// OBJECT SENDER
// ================================================================================================

pub struct ObjectSender {
    /// Address of the peer to which the objects will be sent.
    pub peer_addr: SocketAddr,

    /// IDs for object to be sent by this sender.
    pub object_ids: Vec<ObjectId>,

    /// Whether to delete the objects from the local store after they've been sent.
    pub delete_after_send: bool,

    /// Reference to the plasma store client.
    pub plasma_client: Arc<PlasmaClient>,

    /// Maximum time allocated to retrieving objects from the plasma store.
    pub timeout_ms: i64,

    /// Reference to a set of objects currently scheduled for deletion across all senders.
    pub deleting: Arc<Mutex<HashSet<ObjectId>>>,
}

impl ObjectSender {
    /// Reads objects from the local plasma store and sends them into the specified socket. If
    /// `delete_after_send` = true, it'll try to delete the objects from the store after they
    /// are sent. However, deletion of the objects from the local store is not guaranteed.
    ///
    /// Will return an error if:
    /// * Any of the requested objects are scheduled for deletion.
    /// * Any of the requested objects were not found in the local Plasma Store.
    /// * There was some kind of error retrieving objects from the Plasma Store.
    /// * Any of the requested objects exceed data and metadata size limits.
    /// * Writing objects into the socket fails for some reason; this error may happen after
    ///   some objects have already been written into the socket.
    pub async fn run(&self, socket: &mut TcpStream) -> Result<(), ObjectSendError> {
        // try to send objects and handle any resulting errors
        if let Err(err) = self.send_objects(socket).await {
            // errors which can happen only before any objects are sent will have a response code
            if let Some(response_code) = err.response_code() {
                // if we couldn't send a response code for some reason, there isn't much
                // else we can do - so, just ignore the error
                let _result = socket.write_u8(response_code).await;
            }
            return Err(err);
        }
        Ok(())
    }

    // HELPER METHODS
    // --------------------------------------------------------------------------------------------

    /// Does the actual work described for the `run()` method above.
    async fn send_objects(&self, socket: &mut TcpStream) -> Result<(), ObjectSendError> {
        // save peer address for reporting/debugging purposes
        let num_objects = self.object_ids.len();
        info!("sending {} objects to {}", num_objects, self.peer_addr);

        // make sure none of the objects to be sent are currently scheduled for deletion;
        // if delete_after_send = true and none of the objects are scheduled for deletion,
        // this will also add the object IDs to the set of objects scheduled for deletion
        self.check_deleting()?;

        // get all objects from the plasma store; this also ensures that all requested
        // objects exist locally
        let plasma_object_ids = map_object_ids(&self.object_ids);
        let objects = self.get_objects(&plasma_object_ids)?;

        // make sure that data and metadata sizes for all objects do not exceed allowed limits;
        // we do this before we start sending objects to avoid sending some objects and then
        // discovering that some other objects cannot be sent
        self.check_object_sizes(&objects)?;

        // send a flag indicating that we are about to begin sending objects, and then,
        // one-by-one, write objects into the socket
        socket
            .write_u8(status_codes::BEGIN)
            .await
            .map_err(|err| ObjectSendError::ConnectionError(Some(self.peer_addr), err))?;

        let mut bytes_sent = 0;
        for ob in objects.iter() {
            match send_object(ob, socket).await {
                Ok(()) => {
                    debug!("sent object {} to {}", ob, self.peer_addr);
                    bytes_sent += ob.size();
                }
                Err(err) => {
                    // if there was an error sending an object, abort the entire operation
                    return Err(ObjectSendError::ConnectionError(Some(self.peer_addr), err));
                }
            }
        }

        info!(
            "sent {} objects ({} bytes) to {}",
            num_objects, bytes_sent, self.peer_addr
        );

        // if asked, delete the objects from the local plasma store; this does not guarantee
        // that the objects have in fact been deleted since plasma store will silently skip
        // any object which is in use by other clients.
        if self.delete_after_send {
            if let Err(err) = self.plasma_client.delete_many(&plasma_object_ids) {
                error!("error while deleting objects from plasma store: {}", err);
            }
        }

        Ok(())
    }

    /// Checks if any of the IDs in `object_ids` are in the deleting set, and if they are,
    /// returns an error. Also, if `will_delete` = true, the IDs are added to the deleting set.
    fn check_deleting(&self) -> Result<(), ObjectSendError> {
        // ensure thread-safety by acquiring a lock to the set of objects scheduled for
        // deletion; `unwrap()` is OK here because no thread will panic wile holding the lock.
        let mut deleting = self.deleting.lock().unwrap();

        // check if any of the IDs are in the deleting set
        let mut in_deleting = Vec::new();
        for oid in self.object_ids.iter() {
            if deleting.contains(oid) {
                in_deleting.push(*oid);
            }
        }

        // if they were, return an error
        if !in_deleting.is_empty() {
            return Err(ObjectSendError::ObjectDeletionScheduled(
                self.peer_addr,
                in_deleting,
            ));
        }

        // update the deleting set if the objects will be deleted
        if self.delete_after_send {
            deleting.extend(self.object_ids.iter());
        }

        Ok(())
    }

    /// Makes sure that none of the objects in the list is too big (both for data and metadata)
    fn check_object_sizes(&self, objects: &[ObjectBuffer<'_>]) -> Result<(), ObjectSendError> {
        for ob in objects {
            let meta_size = ob.meta().len();
            if meta_size as u64 > MAX_META_SIZE {
                let oid: ObjectId = ob.id().to_bytes().try_into().unwrap();
                return Err(ObjectSendError::ObjectMetaTooLarge(
                    self.peer_addr,
                    oid,
                    meta_size,
                ));
            }
            let data_size = ob.data().len();
            if data_size as u64 > MAX_DATA_SIZE {
                let oid: ObjectId = ob.id().to_bytes().try_into().unwrap();
                return Err(ObjectSendError::ObjectDataTooLarge(
                    self.peer_addr,
                    oid,
                    data_size,
                ));
            }
        }
        Ok(())
    }

    /// Retrieves the specified objects from the local plasma store; this will return an
    /// error if:
    /// * There was some error retrieving objects from the store.
    /// * Some objects could not be found in the store
    fn get_objects(
        &self,
        object_ids: &[plasma_store::ObjectId],
    ) -> Result<Vec<ObjectBuffer>, ObjectSendError> {
        match self.plasma_client.get_many(&object_ids, self.timeout_ms) {
            Ok(objects) => {
                // check if any of the objects were returned as None, and record corresponding
                // IDs in a separate vector
                let mut missing = Vec::new();
                let mut result = Vec::with_capacity(objects.len());
                for (i, ob) in objects.into_iter().enumerate() {
                    match ob {
                        Some(ob) => result.push(ob),
                        None => missing.push(self.object_ids[i]),
                    }
                }

                // if any of the objects were not found, return an error
                if !missing.is_empty() {
                    return Err(ObjectSendError::ObjectsNotFound(self.peer_addr, missing));
                }

                Ok(result)
            }
            Err(err) => Err(ObjectSendError::StoreError(self.peer_addr, err)),
        }
    }
}

impl Drop for ObjectSender {
    /// When the sender is dropped, we may need to remove all sender objects from the deleting set.
    /// We do it here because the deleting set needs to be cleared regardless of whether there were
    /// errors or not.
    fn drop(&mut self) {
        if self.delete_after_send {
            // ensure thread-safety by acquiring a lock to the set of objects scheduled for deletion;
            // `unwrap()` is OK here because no thread will panic wile holding the lock.
            let mut deleting = self.deleting.lock().unwrap();

            // remove all specified objects from the set
            for oid in self.object_ids.iter() {
                deleting.remove(oid);
            }
        }
    }
}

// HELPER FUNCTIONS
// ================================================================================================

/// Writes the object into the socket; the object is written as follows:
/// * first object header (data and meta size) is written as u64
/// * then, object metadata is written,
/// * and finally, object data buffer is written
async fn send_object(ob: &ObjectBuffer<'_>, socket: &mut TcpStream) -> std::io::Result<()> {
    // Write object header into the socket. The object header consists of a 16-bit value
    // describing the size of the metadata, and a 48-bit value describing the size of that
    // data. Thus, object metadata is limited to at most 64 KB, while object data can be
    // potentially as larger as 256 TB (though MAX_DATA_SIZE imposes 16 TB limit).
    // asserts are OK here because we check object sizes beforehand, and asserts should
    // never fail
    let meta_size = ob.meta().len() as u64;
    assert!(meta_size <= MAX_META_SIZE, "object metadata is too large");
    let data_size = ob.data().len() as u64;
    assert!(data_size <= MAX_DATA_SIZE, "object data is too large");
    let header = meta_size | (data_size << 16);
    socket.write_u64_le(header).await?;

    // write both data and metadata into the socket
    socket.write_all(ob.meta()).await?;
    socket.write_all(ob.data()).await?;

    Ok(())
}
