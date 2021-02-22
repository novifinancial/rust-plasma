// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

/// Converts a list of 20-byte arrays into plasma store object IDs.
pub fn map_object_ids(object_ids: &[crate::ObjectId]) -> Vec<plasma_store::ObjectId> {
    object_ids
        .iter()
        .map(|oid| plasma_store::ObjectId::new(*oid))
        .collect()
}
