// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use super::*;
use cxx::UniquePtr;
use rand::Rng;
use std::panic::{self, AssertUnwindSafe};

// OBJECT ID TESTS
// ================================================================================================

#[test]
fn plasma_ffi_oid_from_binary() {
    let _oid: UniquePtr<ffi::ObjectID> = ffi::oid_from_binary(&[
        1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    ]);
}

#[test]
fn plasma_ffi_oid_roundtrip() {
    let oid: UniquePtr<ffi::ObjectID> = get_random_oid();
    let bin = ffi::oid_to_binary(&oid);
    let oid_deser = ffi::oid_from_binary(&bin);
    assert!(ffi::oid_equals(&oid, &oid_deser));
    assert_eq!(bin, ffi::oid_to_binary(&oid_deser));
}

#[test]
fn plasma_ffi_oid_to_hex() {
    let oid: UniquePtr<ffi::ObjectID> = ffi::oid_from_binary(&[
        1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    ]);
    assert_eq!(
        "0102030405060708090a0b0c0d0e0f1011121314",
        ffi::oid_to_hex(&oid)
    );
}

// CLIENT TESTS
// ================================================================================================

#[test]
fn plasma_ffi_new_client() {
    let _pc: UniquePtr<ffi::PlasmaClient> = ffi::new_plasma_client();
}

#[test]
fn plasma_ffi_connect_error() {
    let pc: UniquePtr<ffi::PlasmaClient> = ffi::new_plasma_client();
    assert_eq!(
        ffi::StatusCode::IOError,
        ffi::connect(pc.as_ref().unwrap(), "/dev/null", 0).code
    ); // -> IOError
}

#[test]
fn plasma_ffi_disconnected_store_capacity() {
    let pc: UniquePtr<ffi::PlasmaClient> = ffi::new_plasma_client();
    let val = ffi::store_capacity_bytes(pc.as_ref().unwrap());
    assert_eq!(val, 0); // store is not connected
}

// CONNECTION-REQUIRING TESTS
// ================================================================================================
// tests below require plasma store server to be running on the local machine; building plasma
// server is expensive, and thus these tests are excluded from regular test runs.
// running ignored tests can be done via: cargo test -- --ignored

#[test]
#[ignore]
fn plasma_ffi_connect() {
    let pc: UniquePtr<ffi::PlasmaClient> = ffi::new_plasma_client();
    let res = ffi::connect(pc.as_ref().unwrap(), "/tmp/plasma", 0);
    assert_eq!(res.code, ffi::StatusCode::OK);
}

#[test]
#[ignore]
fn plasma_ffi_connect_disconnect() {
    run_test(|_| {})
}

#[test]
#[ignore]
fn plasma_ffi_create() {
    run_test(|pc| {
        let mut ob = ffi::new_obj_buffer();
        let oid = get_random_oid();
        let meta = vec![1, 3, 5, 7];
        let res2 = ffi::create(pc, ob.pin_mut(), &oid, 8, &meta);

        let data_mut = unsafe { ffi::get_buffer_data_mut(&ob.data) };
        for i in 0..8 {
            data_mut[i] = i as u8;
        }

        assert!(flex_code_check(res2.code));
    })
}

#[test]
#[ignore]
fn plasma_ffi_create_and_seal() {
    run_test(|pc| {
        let oid = get_random_oid();
        let data = [0u8; 32];
        let meta = vec![];
        let res2 = ffi::create_and_seal(pc, &oid, &data, &meta);
        assert!(flex_code_check(res2.code));
    })
}

#[test]
#[ignore]
fn plasma_ffi_get() {
    run_test(|pc| {
        let oid = get_random_oid();
        // put object into the store
        let data = [2u8; 16];
        let meta = vec![1, 2, 3, 4];
        let _ = ffi::create_and_seal(pc, &oid, &data, &meta);

        // get object from the store
        let mut ob = ffi::new_obj_buffer();
        let res2 = ffi::get(pc, &oid, 1, ob.pin_mut());

        assert!(flex_code_check(res2.code));
    })
}

#[test]
#[ignore]
fn plasma_ffi_contains() {
    run_test(|pc| {
        let oid = get_random_oid();
        // put object into the store
        let data = [1u8; 32];
        let meta = vec![];
        let _ = ffi::create_and_seal(pc, &oid, &data, &meta);

        // check if the object is in the store
        let mut contained = false;
        let res = ffi::contains(pc, &oid, &mut contained);
        assert_eq!(res.code, ffi::StatusCode::OK,);
        assert_eq!(contained, true);
    })
}

// HELPER FUNCTIONS
// ================================================================================================

// TODO: automate setting up this connection for tests

fn conn_setup() -> UniquePtr<ffi::PlasmaClient> {
    let pc: UniquePtr<ffi::PlasmaClient> = ffi::new_plasma_client();
    let res1 = ffi::connect(pc.as_ref().unwrap(), "/tmp/plasma", 0);
    assert_eq!(res1.code, ffi::StatusCode::OK);
    pc
}

fn conn_teardown(pc: &ffi::PlasmaClient) {
    let res2 = ffi::disconnect(pc);
    assert_eq!(res2.code, ffi::StatusCode::OK);
}

fn run_test<T>(test: T) -> ()
where
    T: FnOnce(&ffi::PlasmaClient) -> () + panic::UnwindSafe,
{
    let pc = conn_setup();
    let pc_ref = pc.as_ref().unwrap();

    let result = panic::catch_unwind(AssertUnwindSafe(|| test(pc_ref)));

    conn_teardown(pc.as_ref().unwrap());
    assert!(result.is_ok())
}

fn flex_code_check(s: ffi::StatusCode) -> bool {
    // the race condition is irrelevant for these purposes
    s == ffi::StatusCode::AlreadyExists || s == ffi::StatusCode::OK
}

fn get_random_oid() -> UniquePtr<ffi::ObjectID> {
    let bytes: [u8; 20] = rand::thread_rng().gen();
    ffi::oid_from_binary(&bytes)
}
