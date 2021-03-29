// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use cxx::UniquePtr;
use rand::Rng;
use std::fmt::{self, Debug, Display, Formatter};

mod ffi;
use ffi::ffi as plasma;

mod errors;
pub use errors::PlasmaError;

#[cfg(test)]
mod tests;

// OBJECT ID
// ================================================================================================

// this should be OK because an object ID cannot be mutated
unsafe impl Send for plasma::ObjectID {}
unsafe impl Sync for plasma::ObjectID {}

pub struct ObjectId(UniquePtr<plasma::ObjectID>);

impl ObjectId {
    /// Returns a new object ID instantiated from the specified bytes.
    pub fn new(bytes: [u8; 20]) -> Self {
        ObjectId(plasma::oid_from_binary(&bytes))
    }

    /// Returns a new object ID instantiated from a random sequence of 20 bytes.
    pub fn rand() -> Self {
        Self::new(rand::thread_rng().gen())
    }

    /// Returns binary representation of the object ID.
    pub fn to_bytes(&self) -> &[u8] {
        plasma::oid_to_binary(&self.0)
    }

    /// Returns hexadecimal representation fo the object ID.
    pub fn to_hex(&self) -> String {
        plasma::oid_to_hex(&self.0)
    }

    fn inner(&self) -> &UniquePtr<plasma::ObjectID> {
        &self.0
    }
}

impl Clone for ObjectId {
    fn clone(&self) -> Self {
        ObjectId(plasma::oid_from_binary(&self.to_bytes()))
    }
}

impl Debug for ObjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl Display for ObjectId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl PartialEq for ObjectId {
    fn eq(&self, other: &Self) -> bool {
        plasma::oid_equals(&self.0, &other.0)
    }
}

// OBJECT BUFFER
// ================================================================================================

// this should be OK because:
// * ObjectId can never be mutated
// * PlasmaClient is thread-safe on the C++ side
// * Object buffer on the C++ side can be mutated only once, right after it is crated; so, there
//   should never be two mutable references to an object buffer
// * is_mutable and is_aborted can be updated only via mutable references to ObjectBuffer, and
//   thus cannot be done simultaneously from different threads.
unsafe impl<'a> Send for ObjectBuffer<'a> {}
unsafe impl<'a> Sync for ObjectBuffer<'a> {}

pub struct ObjectBuffer<'a> {
    id: ObjectId,
    pc: &'a UniquePtr<plasma::PlasmaClient>,
    buf: UniquePtr<plasma::ObjectBuffer>,
    is_mutable: bool,
    is_aborted: bool,
}

impl<'a> ObjectBuffer<'a> {
    fn new(
        id: ObjectId,
        pc: &'a UniquePtr<plasma::PlasmaClient>,
        buf: UniquePtr<plasma::ObjectBuffer>,
        is_mutable: bool,
    ) -> Self {
        ObjectBuffer {
            id,
            pc,
            buf,
            is_mutable,
            is_aborted: false,
        }
    }

    /// Returns object ID of this object buffer.
    pub fn id(&self) -> &ObjectId {
        &self.id
    }

    /// Returns read-only data buffer of this object buffer.
    pub fn data(&self) -> &[u8] {
        plasma::get_buffer_data(&self.buf.data)
    }

    /// Returns mutable data buffer of this object buffer.
    pub fn data_mut(&mut self) -> &mut [u8] {
        assert!(self.is_mutable, "object buffer is not mutable");
        unsafe { plasma::get_buffer_data_mut(&self.buf.data) }
    }

    /// Returns metadata buffer of this object buffer.
    pub fn meta(&self) -> &[u8] {
        plasma::get_buffer_data(&self.buf.metadata)
    }

    /// Returns the size of this object buffer in bytes; this includes size of data and
    /// metadata.
    pub fn size(&self) -> usize {
        let meta_size = plasma::get_buffer_data(&self.buf.metadata).len();
        let data_size = plasma::get_buffer_data(&self.buf.data).len();
        meta_size + data_size
    }

    /// Returns true if data of this object buffer is mutable.
    pub fn is_mutable(&self) -> bool {
        self.is_mutable
    }

    /// Seals an object in the object store. The object will be immutable after this call.
    pub fn seal(&mut self) -> Result<(), PlasmaError> {
        let status = plasma::seal(self.pc.as_ref().unwrap(), self.id.inner());
        match status.code {
            plasma::StatusCode::OK => {
                self.is_mutable = false;
                Ok(())
            }
            plasma::StatusCode::TypeError => Err(PlasmaError::AlreadySealed),
            _ => Err(PlasmaError::UnknownError(status.msg)),
        }
    }

    /// Aborts an unsealed object in the object store. If the abort succeeds, then
    /// it will be as if the object was never created at all.
    pub fn abort(mut self) -> Result<(), PlasmaError> {
        if !self.is_mutable {
            return Err(PlasmaError::NotMutable);
        }

        // release the object before it is aborted
        let status = plasma::release(self.pc.as_ref().unwrap(), self.id.inner());
        match status.code {
            plasma::StatusCode::OK => {
                // once the object has been released, call abort
                let status = plasma::abort(self.pc.as_ref().unwrap(), self.id.inner());
                match status.code {
                    plasma::StatusCode::OK => {
                        self.is_aborted = true;
                        Ok(())
                    }
                    _ => Err(PlasmaError::UnknownError(status.msg)),
                }
            }
            _ => Err(PlasmaError::UnknownError(format!(
                "release failed: {}",
                status.msg
            ))),
        }
    }
}

impl<'a> Debug for ObjectBuffer<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "(id: {}, size: {})", self.id.to_hex(), self.data().len())
    }
}

impl<'a> Display for ObjectBuffer<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "(id: {}, size: {})", self.id.to_hex(), self.data().len())
    }
}

impl<'a> Drop for ObjectBuffer<'a> {
    fn drop(&mut self) {
        if !self.is_aborted {
            let status = plasma::release(self.pc.as_ref().unwrap(), self.id().inner());
            if let plasma::StatusCode::OK = status.code {
            } else {
                panic!("failed to release object buffer: {}", status.msg);
            }
        }
    }
}

// PLASMA CLIENT
// ================================================================================================

// this should be OK because PlasmaClient is thread-safe on the C++ side.
unsafe impl Send for plasma::PlasmaClient {}
unsafe impl Sync for plasma::PlasmaClient {}

pub struct PlasmaClient {
    socket_name: String,
    client_ptr: UniquePtr<plasma::PlasmaClient>,
}

impl PlasmaClient {
    /// Creates a new client and connects it to the local plasma store.
    /// * `store_socket_name` The name of the UNIX domain socket to use to connect
    ///   to the Plasma store.
    /// * `num_retries` number of attempts to connect to IPC socket, default 50
    pub fn new(store_socket_name: &str, num_retries: u32) -> Result<Self, PlasmaError> {
        let client_ptr = plasma::new_plasma_client();
        let status = plasma::connect(client_ptr.as_ref().unwrap(), store_socket_name, num_retries);
        match status.code {
            plasma::StatusCode::OK => Ok(PlasmaClient {
                socket_name: String::from(store_socket_name),
                client_ptr,
            }),
            _ => Err(PlasmaError::ConnectError(status.msg)),
        }
    }

    /// Sets runtime options for this client.
    /// * The name of the client, used in debug messages.
    /// * The memory quota in bytes for objects created by this client.
    pub fn set_options(
        &mut self,
        client_name: &str,
        output_memory_quota: usize,
    ) -> Result<(), PlasmaError> {
        let status = plasma::set_client_options(
            self.client_ptr.as_ref().unwrap(),
            client_name,
            output_memory_quota as i64,
        );
        match status.code {
            plasma::StatusCode::OK => Ok(()),
            _ => Err(PlasmaError::UnknownError(status.msg)),
        }
    }

    /// Retrieves an object with the specified ID from the store. This function will block until
    /// the object has been created and sealed in the Plasma store or the timeout expires.
    /// * `oid` The ID of the object to get.
    /// * `timeout_ms` The amount of time in milliseconds to wait before this request times out.
    ///    If this value is -1, then no timeout is set.
    pub fn get(&self, oid: ObjectId, timeout_ms: i64) -> Result<Option<ObjectBuffer>, PlasmaError> {
        let mut ob = plasma::new_obj_buffer();
        let status = plasma::get(
            self.client_ptr.as_ref().unwrap(),
            oid.inner(),
            timeout_ms,
            ob.pin_mut(),
        );
        match status.code {
            plasma::StatusCode::OK => {
                if ob.data.is_null() {
                    Ok(None)
                } else {
                    Ok(Some(ObjectBuffer::new(oid, &self.client_ptr, ob, false)))
                }
            }
            _ => Err(PlasmaError::UnknownError(status.msg)),
        }
    }

    /// Retrieves a list of specified objects from the store.This function will block until
    /// all objects have been created and sealed in the Plasma store or the timeout expires.
    /// * `object_ids` The list of IDs for objects to get.
    /// * `timeout_ms` The amount of time in milliseconds to wait before this request times out.
    ///    If this value is -1, then no timeout is set.
    pub fn get_many(
        &self,
        object_ids: &[ObjectId],
        timeout_ms: i64,
    ) -> Result<Vec<Option<ObjectBuffer>>, PlasmaError> {
        // TODO: use native C++ function to retrieve all objects at once
        let mut result = Vec::with_capacity(object_ids.len());
        for oid in object_ids {
            result.push(self.get(oid.clone(), timeout_ms)?);
        }
        Ok(result)
    }

    /// Creates an object in the Plasma Store. Any metadata for this object must be
    /// passed in when the object is created.
    /// * `oid` The ID to use for the newly crated object.
    /// * `data_size` The size in bytes of the space to be allocated for this object's data
    ///     (this does not included space used for metadata).
    /// * `meta` The object's metadata; if there is no metadata, this should be an empty slice.
    ///
    /// The returned object must be either sealed or aborted when done with.
    pub fn create(
        &self,
        oid: ObjectId,
        data_size: usize,
        meta: &[u8],
    ) -> Result<ObjectBuffer, PlasmaError> {
        let mut ob = plasma::new_obj_buffer();
        let status = plasma::create(
            self.client_ptr.as_ref().unwrap(),
            ob.pin_mut(),
            oid.inner(),
            data_size as i64,
            meta,
        );
        match status.code {
            plasma::StatusCode::OK => Ok(ObjectBuffer::new(oid, &self.client_ptr, ob, true)),
            plasma::StatusCode::AlreadyExists => Err(PlasmaError::AlreadyExists),
            _ => Err(PlasmaError::UnknownError(status.msg)),
        }
    }

    /// Creates and seals an object in the object store. This is an optimization which allows
    /// small objects to be created quickly with fewer messages to the store.
    /// * `oid` The ID for the object to create.
    /// * `data` The data for the object to create.
    /// * `meta` The metadata for the object to create.
    pub fn create_and_seal(
        &self,
        oid: ObjectId,
        data: &[u8],
        meta: &[u8],
    ) -> Result<(), PlasmaError> {
        let status =
            plasma::create_and_seal(self.client_ptr.as_ref().unwrap(), oid.inner(), data, meta);
        match status.code {
            plasma::StatusCode::OK => Ok(()),
            plasma::StatusCode::AlreadyExists => Err(PlasmaError::AlreadyExists),
            _ => Err(PlasmaError::UnknownError(status.msg)),
        }
    }

    /// Deletes an object from the object store. This currently assumes that the
    /// object is present, has been sealed and not used by another client. Otherwise,
    /// it is a no operation.
    pub fn delete(&self, oid: &ObjectId) -> Result<(), PlasmaError> {
        let status = plasma::delete(self.client_ptr.as_ref().unwrap(), oid.inner());
        match status.code {
            plasma::StatusCode::OK => Ok(()),
            _ => Err(PlasmaError::UnknownError(status.msg)),
        }
    }

    /// Deletes all objects specified by `object_ids` list from the object store. This
    /// currently assumes that the objects are present, haven been sealed and are not
    /// used by another client. Otherwise it is a no operation.
    pub fn delete_many(&self, object_ids: &[ObjectId]) -> Result<(), PlasmaError> {
        // TODO: use native C++ function to retrieve all objects at once
        for oid in object_ids {
            self.delete(oid)?;
        }
        Ok(())
    }

    /// Checks if the object store contains a particular object and the object has been sealed.
    pub fn contains(&self, oid: &ObjectId) -> Result<bool, PlasmaError> {
        let mut has_object = false;
        let status = plasma::contains(
            self.client_ptr.as_ref().unwrap(),
            oid.inner(),
            &mut has_object,
        );
        match status.code {
            plasma::StatusCode::OK => Ok(has_object),
            _ => Err(PlasmaError::UnknownError(status.msg)),
        }
    }

    /// Returns a list of IDs for objects contained in the object store.
    pub fn contains_many(&self, object_ids: &[ObjectId]) -> Result<Vec<ObjectId>, PlasmaError> {
        let mut found_objects = Vec::new();
        // TODO: move this to C++ side to make it more efficient?
        for oid in object_ids.iter() {
            if self.contains(oid)? {
                found_objects.push(oid.clone());
            }
        }
        Ok(found_objects)
    }

    /// Returns memory capacity of the store in bytes.
    pub fn store_capacity(&self) -> usize {
        plasma::store_capacity_bytes(self.client_ptr.as_ref().unwrap()) as usize
    }
}

impl Drop for PlasmaClient {
    fn drop(&mut self) {
        plasma::disconnect(self.client_ptr.as_ref().unwrap());
    }
}

impl Debug for PlasmaClient {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "PlasmaClient {{ socket: {} }}", self.socket_name)
    }
}
