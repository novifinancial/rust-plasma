// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use super::{ObjectId, ObjectReceiver, ObjectSender};
use plasma_store::PlasmaClient;
use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

// OBJECT STORE WRAPPER
// ================================================================================================

#[derive(Debug, Clone)]
pub struct Store {
    /// Connection to the Plasma Store. We put it into an Arc because it can be accessed from
    /// multiple threads concurrently, and we don't want to clone the connection for each thread.
    plasma_client: Arc<PlasmaClient>,

    /// Maximum time allocated to retrieving objects from the store.
    timeout_ms: i64,

    /// A set of IDs for objects which are in the process of being received. This is used to
    /// make sure two separate requests don't try to receive the same object.
    // TODO: use non-cryptographic hashing
    receiving: Arc<Mutex<HashSet<ObjectId>>>,

    /// A set of IDs for objects which are scheduled to be deleted. This is used to make sure
    /// two separate requests don't try to delete the same object from the store.
    // TODO: use non-cryptographic hashing
    deleting: Arc<Mutex<HashSet<ObjectId>>>,
}

impl Store {
    pub fn new(plasma_client: PlasmaClient, timeout_ms: i64) -> Self {
        Store {
            plasma_client: Arc::new(plasma_client),
            timeout_ms,
            receiving: Arc::new(Mutex::new(HashSet::new())),
            deleting: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Returns a new ObjectSender for sending objects with the specified IDs.
    pub fn build_sender(
        &self,
        peer_addr: SocketAddr,
        object_ids: Vec<ObjectId>,
        delete_after_send: bool,
    ) -> ObjectSender {
        ObjectSender {
            peer_addr,
            object_ids,
            delete_after_send,
            plasma_client: self.plasma_client.clone(),
            timeout_ms: self.timeout_ms,
            deleting: self.deleting.clone(),
        }
    }

    /// Returns a new ObjectReceiver for receiving objects with the specified IDs.
    pub fn build_receiver(
        &self,
        peer_addr: SocketAddr,
        object_ids: Vec<ObjectId>,
    ) -> ObjectReceiver {
        ObjectReceiver {
            peer_addr,
            object_ids,
            plasma_client: self.plasma_client.clone(),
            receiving: self.receiving.clone(),
        }
    }
}
