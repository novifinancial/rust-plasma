// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

#[cfg(test)]
mod tests;

#[allow(clippy::all)]
#[cxx::bridge(namespace = plasma)]
pub(crate) mod ffi {

    /// Object buffer data structure.
    struct ObjectBuffer {
        /// The data buffer.
        data: SharedPtr<Buffer>,
        /// The metadata buffer.
        metadata: SharedPtr<Buffer>,
        /// The device number.
        device_num: i32,
    }

    #[derive(Debug)]
    pub struct ArrowStatus {
        code: StatusCode,
        msg: String,
    }

    #[derive(Debug)]
    enum StatusCode {
        OK,
        OutOfMemory,
        KeyError,
        TypeError,
        Invalid,
        IOError,
        CapacityError,
        IndexError,
        UnknownError,
        NotImplemented,
        SerializationError,
        RError,                    // 13
        CodeGenError,              // 40
        ExpressionValidationError, // 41
        ExecutionError,            // 42
        AlreadyExists,             // 45
    }

    unsafe extern "C++" {
        include!("src/ffi/ffi.h");

        type ObjectID;

        fn oid_from_binary(binary: &[u8]) -> UniquePtr<ObjectID>;
        fn oid_to_binary(oid: &ObjectID) -> &[u8];
        fn oid_to_hex(oid: &ObjectID) -> String;
        fn oid_equals(oid1: &ObjectID, oid2: &ObjectID) -> bool;

        #[namespace = "arrow"]
        type Buffer;

        fn get_buffer_data(buffer: &SharedPtr<Buffer>) -> &[u8];

        // Safety:
        //   - buffer must be a mutable CPU buffer
        //   - caller must not obtain overlapping slices of the same buffer
        unsafe fn get_buffer_data_mut(buffer: &SharedPtr<Buffer>) -> &mut [u8];

        #[namespace = "arrow"]
        type MutableBuffer;

        type ObjectBuffer;
        fn new_obj_buffer() -> UniquePtr<ObjectBuffer>;

        type PlasmaClient;

        fn new_plasma_client() -> UniquePtr<PlasmaClient>;
        fn connect(pc: &PlasmaClient, store_socket_name: &str, num_retries: u32) -> ArrowStatus;

        fn set_client_options(
            pc: &PlasmaClient,
            client_name: &str,
            output_memory_quota: i64,
        ) -> ArrowStatus;

        fn create(
            pc: &PlasmaClient,
            ob: Pin<&mut ObjectBuffer>,
            oid: &ObjectID,
            data_size: i64,
            metadata: &[u8],
        ) -> ArrowStatus;

        fn create_and_seal(
            pc: &PlasmaClient,
            oid: &ObjectID,
            data: &[u8],
            metadata: &[u8],
        ) -> ArrowStatus;

        fn get(
            pc: &PlasmaClient,
            oid: &ObjectID,
            timeout_ms: i64,
            ob: Pin<&mut ObjectBuffer>,
        ) -> ArrowStatus;

        // TODO: implement multi_get abstraction
        #[allow(dead_code)]
        fn multi_get(
            pc: &PlasmaClient,
            oids: &CxxVector<ObjectID>,
            timeout_ms: i64,
            obs: Pin<&mut CxxVector<ObjectBuffer>>,
        ) -> ArrowStatus;

        fn release(pc: &PlasmaClient, oid: &ObjectID) -> ArrowStatus;

        fn contains(pc: &PlasmaClient, oid: &ObjectID, has_object: &mut bool) -> ArrowStatus;

        fn abort(pc: &PlasmaClient, oid: &ObjectID) -> ArrowStatus;

        fn seal(pc: &PlasmaClient, oid: &ObjectID) -> ArrowStatus;

        #[cxx_name = "single_delete"]
        fn delete(pc: &PlasmaClient, oid: &ObjectID) -> ArrowStatus;

        // TODO: implement multi_delete abstraction
        #[allow(dead_code)]
        fn multi_delete(pc: &PlasmaClient, oid: &CxxVector<ObjectID>) -> ArrowStatus;

        // TODO: implement refresh abstraction
        #[allow(dead_code)]
        fn refresh(pc: &PlasmaClient, oid: &CxxVector<ObjectID>) -> ArrowStatus;

        fn disconnect(pc: &PlasmaClient) -> ArrowStatus;

        fn store_capacity_bytes(pc: &PlasmaClient) -> i64;
    }
}
