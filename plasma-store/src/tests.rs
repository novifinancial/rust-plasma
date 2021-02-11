// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

use super::*;

/// CONSTANTS
/// ===============================================================================================

const PLASMA_SOCKET: &str = "/tmp/plasma";

/// OBJECT ID TESTS
/// ===============================================================================================

#[test]
fn plasma_object_id_new() {
    let bytes = [
        1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    ];
    let oid = ObjectId::new(bytes);
    assert_eq!(oid.to_bytes(), bytes);
    assert_eq!("0102030405060708090a0b0c0d0e0f1011121314", oid.to_hex());
}

#[test]
fn plasma_object_id_rand() {
    let oid1 = ObjectId::rand();
    let oid2 = ObjectId::rand();
    assert_ne!(oid1, oid2);
}

#[test]
fn plasma_object_id_clone() {
    let oid1 = ObjectId::rand();
    let oid2 = oid1.clone();
    assert_eq!(oid1, oid2);
}

/// CLIENT TESTS
/// ===============================================================================================
// tests below require plasma store server to be running on the local machine; building plasma
// server is expensive, and thus these tests are excluded from regular test runs.
// running ignored tests can be done via: cargo test -- --ignored

#[test]
#[ignore]
fn plasma_client_new() {
    assert_eq!(true, PlasmaClient::new(PLASMA_SOCKET, 0).is_ok());
    assert_eq!(true, PlasmaClient::new("/tmp/plasma2", 0).is_err());
}

#[test]
#[ignore]
fn plasma_client_create_and_seal() {
    let pc = build_client();
    let oid = ObjectId::rand();
    let data = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let meta = [1, 2, 3, 4];

    assert!(pc.create_and_seal(oid.clone(), &data, &meta).is_ok());

    // creating an object with the same ID should result in an error
    assert!(pc.create_and_seal(oid.clone(), &data, &meta).is_err());
}

#[test]
#[ignore]
fn plasma_client_get() {
    let pc = build_client();

    // put object into the store
    let oid = ObjectId::rand();
    let data = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let meta = [1, 2, 3, 4];
    pc.create_and_seal(oid.clone(), &data, &meta).unwrap();

    // get object out of the store and make sure data and metadata are the same
    let ob = pc.get(oid, 5).unwrap().unwrap();
    assert_eq!(data, ob.data(), "object data should match");
    assert_eq!(meta, ob.meta(), "object metadata should match");
    assert_eq!(false, ob.is_mutable(), "object should not be mutable");

    // if we try to retrieve a non-existent object, we should get None back
    let ob = pc.get(ObjectId::rand(), 5).unwrap();
    assert!(ob.is_none());
}

#[test]
#[ignore]
fn plasma_client_get_many() {
    let pc = build_client();

    let meta = [1, 2, 3, 4];

    // put objects into the store
    let oid1 = ObjectId::rand();
    let data1 = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    pc.create_and_seal(oid1.clone(), &data1, &meta).unwrap();

    let oid2 = ObjectId::rand();
    let data2 = [1u8, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31];
    pc.create_and_seal(oid2.clone(), &data2, &meta).unwrap();

    // get objects out of the store and make sure they are returned correctly
    let oids = [oid1, oid2, ObjectId::rand()];
    let mut result = pc.get_many(&oids, 5).unwrap();
    assert_eq!(
        oids.len(),
        result.len(),
        "number of results and IDs should match"
    );
    assert!(result[0].is_some(), "first result should be Some");
    assert!(result[1].is_some(), "second result should be Some");
    assert!(result[2].is_none(), "third result should be Some");

    assert_eq!(
        data1,
        result[0].take().unwrap().data(),
        "object1 data should match"
    );
    assert_eq!(
        data2,
        result[1].take().unwrap().data(),
        "object2 data should match"
    );
}

#[test]
#[ignore]
fn plasma_client_contains() {
    let pc = build_client();

    let oid = ObjectId::rand();

    // make sure the object is not in the store
    assert_eq!(
        false,
        pc.contains(&oid).unwrap(),
        "object should not be in the store"
    );

    // put object into the store
    let data = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    pc.create_and_seal(oid.clone(), &data, &[]).unwrap();

    // make sure the object is in the store
    assert_eq!(
        true,
        pc.contains(&oid).unwrap(),
        "object should be in the store"
    );
}

#[test]
#[ignore]
fn plasma_client_contains_many() {
    let pc = build_client();

    let meta = [1, 2, 3, 4];

    // put objects into the store
    let oid1 = ObjectId::rand();
    let data1 = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    pc.create_and_seal(oid1.clone(), &data1, &meta).unwrap();

    let oid2 = ObjectId::rand();
    let data2 = [1u8, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31];
    pc.create_and_seal(oid2.clone(), &data2, &meta).unwrap();

    // check which objects are in the store
    let oids = [oid1.clone(), oid2.clone(), ObjectId::rand()];
    let result = pc.contains_many(&oids).unwrap();
    assert_eq!(2, result.len(), "two results should be found");
    assert_eq!(oid1, result[0], "oid1 data should match");
    assert_eq!(oid2, result[1], "oid2 data should match");
}

#[test]
#[ignore]
fn plasma_client_create_then_seal() {
    let pc = build_client();
    let pc2 = build_client();

    // create an object of a given size
    let oid = ObjectId::rand();
    let data_size = 16;
    let meta = [1, 2, 3, 4];
    let mut ob = pc.create(oid.clone(), data_size, &meta).unwrap();

    assert_eq!(true, ob.is_mutable(), "object should be mutable");
    assert_eq!(meta, ob.meta(), "object metadata should match");
    assert_eq!(
        data_size,
        ob.data().len(),
        "object data buffer should be of correct length"
    );
    assert_eq!(
        false,
        pc2.contains(ob.id()).unwrap(),
        "client2: object should not be in the store"
    );

    // update data buffer and seal the object
    let data = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let data_buf = ob.data_mut();
    for i in 0..data_buf.len() {
        data_buf[i] = data[i];
    }
    ob.seal().unwrap();

    assert_eq!(false, ob.is_mutable(), "object should not be mutable");
    assert_eq!(data, ob.data(), "object data should match");
    assert_eq!(
        true,
        pc2.contains(ob.id()).unwrap(),
        "object should be in the store"
    );

    // trying to seal twice should result in an error
    assert!(ob.seal().is_err());

    // make sure the object can be retrieved correctly from another client
    let ob = pc2.get(oid, 5).unwrap().unwrap();
    assert_eq!(data, ob.data(), "client2: object data should match");
    assert_eq!(meta, ob.meta(), "client2: object metadata should match");
    assert_eq!(
        false,
        ob.is_mutable(),
        "client2: object should not be mutable"
    );
}

#[test]
#[ignore]
fn plasma_client_create_then_seal_error() {
    let pc = build_client();

    // put object into the store
    let oid = ObjectId::rand();
    let data = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    pc.create_and_seal(oid.clone(), &data, &[]).unwrap();

    // get the object from the store
    let mut ob = pc.get(oid, 5).unwrap().unwrap();

    // trying to seal this object should result in an error
    assert!(ob.seal().is_err());
}

#[test]
#[ignore]
fn plasma_client_create_then_abort() {
    let pc = build_client();

    // create an object of a given size
    let oid = ObjectId::rand();
    let data_size = 16;
    let meta = [1, 2, 3, 4];
    let mut ob = pc.create(oid.clone(), data_size, &meta).unwrap();

    // write data into the object's data buffer
    let data = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let data_buf = ob.data_mut();
    for i in 0..data_buf.len() {
        data_buf[i] = data[i];
    }

    // abort the object
    ob.abort().unwrap();
    assert_eq!(
        false,
        pc.contains(&oid).unwrap(),
        "object should not be in the store"
    );
}

#[test]
#[ignore]
fn plasma_client_create_error() {
    let pc = build_client();

    // put an object into the store
    let oid = ObjectId::rand();
    let data = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    pc.create_and_seal(oid.clone(), &data, &[]).unwrap();

    // try to create an object with the same ID
    assert!(pc.create(oid.clone(), 16, &[]).is_err());
}

#[test]
#[ignore]
fn plasma_client_delete() {
    let pc = build_client();
    let pc2 = build_client();
    let oid = ObjectId::rand();

    // put object into the store
    let data = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    pc.create_and_seal(oid.clone(), &data, &[]).unwrap();
    assert_eq!(
        true,
        pc.contains(&oid).unwrap(),
        "object should be in the store"
    );

    // delete the object
    pc.delete(&oid).unwrap();
    assert_eq!(
        false,
        pc.contains(&oid).unwrap(),
        "object should not be in the store"
    );
    assert_eq!(
        false,
        pc2.contains(&oid).unwrap(),
        "client2: object should not be in the store"
    );
}

#[test]
#[ignore]
fn plasma_client_delete_many() {
    let pc = build_client();

    let meta = [1, 2, 3, 4];

    // put objects into the store
    let oid1 = ObjectId::rand();
    let data1 = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    pc.create_and_seal(oid1.clone(), &data1, &meta).unwrap();

    let oid2 = ObjectId::rand();
    let data2 = [1u8, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31];
    pc.create_and_seal(oid2.clone(), &data2, &meta).unwrap();

    // delete the objects from the store
    let oids = [oid1.clone(), oid2.clone(), ObjectId::rand()];
    pc.delete_many(&oids).unwrap();

    let result = pc.contains_many(&oids).unwrap();
    assert_eq!(0, result.len(), "all objects should be deleted");
}

/// HELPER FUNCTIONS
/// ===============================================================================================

fn build_client() -> PlasmaClient {
    PlasmaClient::new(PLASMA_SOCKET, 0).unwrap()
}
