// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

#include "src/ffi/ffi.h"
#include "plasma-store/src/ffi/mod.rs.h"

namespace plasma {

  //////////////
  // ObjectID //
  //////////////

  std::unique_ptr<ObjectID> oid_from_binary(rust::Slice<const uint8_t> binary) {
    std::string bin_str = std::string(reinterpret_cast<const char*>(binary.data()), binary.size());
    ObjectID oid = plasma::ObjectID::from_binary(bin_str);
    return std::make_unique<ObjectID>(oid);
  }

  rust::Slice<const uint8_t> oid_to_binary(const ObjectID& oid) {
    return rust::Slice<const uint8_t>(oid.data(), kUniqueIDSize);
  }

  rust::String oid_to_hex(const ObjectID& oid) {
    std::string hex = oid.hex();
    return rust::String(hex);
  }

  bool oid_equals(const ObjectID& oid1, const ObjectID& oid2) {
    return oid1 == oid2;
  }

  ////////////
  // Buffer //
  ////////////

  std::unique_ptr<ObjectBuffer> new_obj_buffer() {
    std::shared_ptr<Buffer> data_ptr;
    std::shared_ptr<Buffer> metadata_ptr;
    return std::make_unique<ObjectBuffer>(ObjectBuffer{data_ptr, metadata_ptr, 0});
  }

  rust::Slice<const unsigned char> get_buffer_data(const std::shared_ptr<Buffer>& buffer) {
    const uint8_t *c = buffer->data();
    int64_t len = buffer->size();
    return rust::Slice<const unsigned char>(c, len);
  }

  rust::Slice<unsigned char> get_buffer_data_mut(const std::shared_ptr<Buffer>& buffer) {
    uint8_t *c = buffer->mutable_data();
    int64_t len = buffer->size();
    return rust::Slice<unsigned char>(c, len);
  }

  //////////////////
  // PlasmaClient //
  //////////////////

  std::unique_ptr<PlasmaClient> new_plasma_client() {
    return std::make_unique<PlasmaClient>(plasma::PlasmaClient());
  }

  ArrowStatus connect(PlasmaClient const& pc, rust::Str store_socket_name, uint32_t num_retries) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    std::string manager_socket("");
    Status conn_status = pc_mut.Connect(std::string(store_socket_name), manager_socket, 0, num_retries);
    return ArrowStatus{make_plasma_error(conn_status.code()), conn_status.message()};
  }

  ArrowStatus set_client_options(PlasmaClient const& pc, rust::Str client_name, int64_t output_memory_quota) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    Status client_status = pc_mut.SetClientOptions(std::string(client_name), output_memory_quota);
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  ArrowStatus create(PlasmaClient const& pc, ObjectBuffer& ob, const ObjectID& oid, int64_t data_size, rust::Slice<const uint8_t> metadata) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    std::shared_ptr<Buffer>* data_ptr = &ob.data;
    Status client_status = pc_mut.Create(oid, data_size, metadata.data(), metadata.size(), data_ptr, 0, true);
    ob.metadata = std::make_shared<Buffer>(metadata.data(), metadata.size());
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  ArrowStatus create_and_seal(PlasmaClient const& pc, const ObjectID& oid, rust::Slice<const uint8_t> data, rust::Slice<const uint8_t> metadata) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    std::string bin_data = std::string(reinterpret_cast<const char*>(data.data()), data.size());
    std::string bin_metadata = std::string(reinterpret_cast<const char*>(metadata.data()), metadata.size());

    Status client_status = pc_mut.CreateAndSeal(oid, bin_data, bin_metadata, true);
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  ArrowStatus get(PlasmaClient const& pc, const ObjectID& oid, int64_t timeout_ms, ObjectBuffer& ob) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    const ObjectID* oidp = &oid;
    ObjectBuffer* obp = &ob;
    Status client_status = pc_mut.Get(oidp, 1, timeout_ms, obp);
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  ArrowStatus multi_get(PlasmaClient const& pc, const std::vector<ObjectID>& oids, int64_t timeout_ms, std::vector<ObjectBuffer>& obs) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    std::vector<ObjectBuffer>* buffers = &obs;
    Status client_status = pc_mut.Get(oids, timeout_ms, buffers);
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  ArrowStatus release(PlasmaClient const& pc, const ObjectID& oid) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    Status client_status = pc_mut.Release(oid);
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  ArrowStatus contains(PlasmaClient const& pc, const ObjectID& oid, bool& has_object) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    bool* has_res = &has_object;
    Status client_status = pc_mut.Contains(oid, has_res);
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  ArrowStatus abort(PlasmaClient const& pc, const ObjectID& oid) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    Status client_status = pc_mut.Abort(oid);
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  ArrowStatus seal(PlasmaClient const& pc, const ObjectID& oid) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    Status client_status = pc_mut.Seal(oid);
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  ArrowStatus single_delete(PlasmaClient const& pc, const ObjectID& oid) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    Status client_status = pc_mut.Delete(oid);
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  ArrowStatus multi_delete(PlasmaClient const& pc, const std::vector<ObjectID>& oids) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    Status client_status = pc_mut.Delete(oids);
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  ArrowStatus refresh(PlasmaClient const& pc, const std::vector<ObjectID>& oids) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    Status client_status = pc_mut.Refresh(oids);
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  ArrowStatus disconnect(PlasmaClient const& pc) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    Status client_status = pc_mut.Disconnect();
    return ArrowStatus{make_plasma_error(client_status.code()), client_status.message()};
  }

  int64_t store_capacity_bytes(PlasmaClient const& pc) {
    auto pc_mut = const_cast<PlasmaClient&>(pc);
    return pc_mut.store_capacity();
  }

  ///////////
  // utils //
  ///////////

  StatusCode make_plasma_error(arrow::StatusCode code) {
    StatusCode plasma_code = StatusCode::UnknownError;
    switch (code) {
    case arrow::StatusCode::OK:
      plasma_code = StatusCode::OK;
      break;
    case arrow::StatusCode::OutOfMemory:
      plasma_code = StatusCode::OutOfMemory;
      break;
    case arrow::StatusCode::KeyError:
      plasma_code = StatusCode::KeyError;
      break;
    case arrow::StatusCode::TypeError:
      plasma_code = StatusCode::TypeError;
      break;
    case arrow::StatusCode::Invalid:
      plasma_code = StatusCode::Invalid;
      break;
    case arrow::StatusCode::IOError:
      plasma_code = StatusCode::IOError;
      break;
    case arrow::StatusCode::CapacityError:
      plasma_code = StatusCode::CapacityError;
      break;
    case arrow::StatusCode::IndexError:
      plasma_code = StatusCode::IndexError;
      break;
    case arrow::StatusCode::UnknownError:
      plasma_code = StatusCode::UnknownError;
      break;
    case arrow::StatusCode::NotImplemented:
      plasma_code = StatusCode::NotImplemented;
      break;
    case arrow::StatusCode::SerializationError:
      plasma_code = StatusCode::SerializationError;
      break;
    case arrow::StatusCode::RError:
      plasma_code = StatusCode::RError;
      break;
    case arrow::StatusCode::CodeGenError:
      plasma_code = StatusCode::CodeGenError;
      break;
    case arrow::StatusCode::ExpressionValidationError:
      plasma_code = StatusCode::ExpressionValidationError;
      break;
    case arrow::StatusCode::ExecutionError:
      plasma_code = StatusCode::ExecutionError;
      break;
    case arrow::StatusCode::AlreadyExists:
      plasma_code = StatusCode::AlreadyExists;
      break;
    }
    return plasma_code;
  }


}
