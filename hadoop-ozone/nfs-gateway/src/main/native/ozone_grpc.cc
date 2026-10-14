/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ozone_grpc.h"
#include "NfsGatewayProtocol.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <chrono>
#include <cstring>
#include <memory>
#include <new>

struct ozone_grpc_client {
  std::unique_ptr<ozone::nfs::v1::NfsGateway::Stub> stub;
  uint32_t timeout_millis;
};

static int grpc_status(const grpc::Status &status, uint32_t *nfs_status) {
  if (status.error_code() == grpc::StatusCode::PERMISSION_DENIED) {
    *nfs_status = 13;  // NFS4ERR_ACCESS
    return grpc::StatusCode::OK;
  }
  return status.error_code();
}

static bool valid_caller(const char *principal, size_t principal_size, uint32_t protection) {
  return principal && principal_size && principal_size <= OZONE_NFS_MAX_OWNER_BYTES
      && (protection == 1 || protection == 2);
}

static void set_context(ozone::nfs::v1::RequestContext *context, const uint8_t export_uuid[16],
    const char *principal, size_t principal_size, uint32_t protection) {
  context->set_version(1);
  context->set_export_uuid(export_uuid, 16);
  context->mutable_caller()->set_kerberos_principal(principal, principal_size);
  context->mutable_caller()->set_protection(
      static_cast<ozone::nfs::v1::Caller_Protection>(protection));
}

static bool copy_attributes(const ozone::nfs::v1::Attributes &source,
    struct ozone_nfs_attributes *target) {
  if (source.handle().size() != OZONE_NFS_HANDLE_BYTES ||
      (source.has_owner() && source.owner().size() > OZONE_NFS_MAX_OWNER_BYTES)) {
    return false;
  }
  std::memcpy(target->handle, source.handle().data(), OZONE_NFS_HANDLE_BYTES);
  target->directory = source.directory() ? 1 : 0;
  target->object_id = source.object_id();
  target->change_id = source.change_id();
  target->size = source.size();
  target->modification_time_millis = source.modification_time_millis();
  target->creation_time_millis = source.creation_time_millis();
  if (source.has_owner()) {
    target->owner_size = source.owner().size();
    std::memcpy(target->owner, source.owner().data(), target->owner_size);
  }
  return true;
}

extern "C" struct ozone_grpc_client *ozone_grpc_client_create(
    const char *address, const char *trusted_ca_pem,
    const char *client_certificate_pem, const char *client_key_pem,
    uint32_t timeout_millis) {
  if (!address || !*address || !trusted_ca_pem || !*trusted_ca_pem ||
      !client_certificate_pem || !*client_certificate_pem ||
      !client_key_pem || !*client_key_pem || !timeout_millis ||
      timeout_millis > 60000) {
    return nullptr;
  }
  try {
    grpc::SslCredentialsOptions options;
    options.pem_root_certs = trusted_ca_pem;
    options.pem_cert_chain = client_certificate_pem;
    options.pem_private_key = client_key_pem;
    grpc::ChannelArguments arguments;
    arguments.SetMaxReceiveMessageSize(4 * 1024 * 1024);
    arguments.SetMaxSendMessageSize(4 * 1024 * 1024);
    auto credentials = grpc::SslCredentials(options);
    if (!credentials) return nullptr;
    auto channel = grpc::CreateCustomChannel(address, credentials, arguments);
    if (!channel) return nullptr;
    auto client = std::make_unique<ozone_grpc_client>();
    client->stub = ozone::nfs::v1::NfsGateway::NewStub(channel);
    client->timeout_millis = timeout_millis;
    return client.release();
  } catch (...) {
    return nullptr;
  }
}

extern "C" void ozone_grpc_client_destroy(struct ozone_grpc_client *client) {
  delete client;
}

extern "C" int ozone_grpc_capabilities(struct ozone_grpc_client *client,
    const uint8_t export_uuid[16], uint32_t version,
    uint32_t *nfs_status, uint64_t *operations) {
  if (nfs_status) *nfs_status = 5;
  if (operations) *operations = 0;
  if (!client || !export_uuid || !nfs_status || !operations || !version) {
    return grpc::StatusCode::INVALID_ARGUMENT;
  }
  try {
    ozone::nfs::v1::CapabilityRequest request;
    request.set_version(version);
    request.set_export_uuid(export_uuid, 16);
    ozone::nfs::v1::CapabilityResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
        std::chrono::milliseconds(client->timeout_millis));
    grpc::Status status = client->stub->Capabilities(&context, request, &response);
    if (!status.ok()) return grpc_status(status, nfs_status);
    if (!response.IsInitialized() || response.version() != version) {
      return grpc::StatusCode::FAILED_PRECONDITION;
    }
    uint64_t supported = 0;
    for (int operation : response.operations()) {
      if (operation < 1 || operation > 8) return grpc::StatusCode::FAILED_PRECONDITION;
      supported |= UINT64_C(1) << operation;
    }
    *nfs_status = response.result().nfs_status();
    *operations = *nfs_status == 0 ? supported : 0;
    return grpc::StatusCode::OK;
  } catch (...) {
    return grpc::StatusCode::INTERNAL;
  }
}

extern "C" int ozone_grpc_read(struct ozone_grpc_client *client,
    const uint8_t export_uuid[16], const char *kerberos_principal,
    size_t kerberos_principal_size, uint32_t protection, const uint8_t *handle, size_t handle_size,
    uint64_t offset, uint32_t count, uint8_t *data, size_t data_capacity,
    size_t *bytes_read, uint32_t *eof, uint32_t *nfs_status) {
  if (bytes_read) *bytes_read = 0;
  if (eof) *eof = 0;
  if (nfs_status) *nfs_status = 5;
  if (!client || !export_uuid || !valid_caller(kerberos_principal, kerberos_principal_size, protection) ||
      !handle || handle_size != OZONE_NFS_HANDLE_BYTES || count > OZONE_NFS_MAX_READ_BYTES || data_capacity < count ||
      (count && !data) || !bytes_read || !eof || !nfs_status) {
    return grpc::StatusCode::INVALID_ARGUMENT;
  }
  try {
    ozone::nfs::v1::ReadRequest request;
    set_context(request.mutable_context(), export_uuid, kerberos_principal, kerberos_principal_size, protection);
    request.set_handle(handle, handle_size);
    request.set_offset(offset);
    request.set_count(count);
    ozone::nfs::v1::ReadResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
        std::chrono::milliseconds(client->timeout_millis));
    grpc::Status status = client->stub->Read(&context, request, &response);
    if (!status.ok()) return grpc_status(status, nfs_status);
    if (!response.IsInitialized()) return grpc::StatusCode::FAILED_PRECONDITION;
    *nfs_status = response.result().nfs_status();
    if (*nfs_status != 0) {
      if (response.has_data() || response.has_eof()) {
        *nfs_status = 5;
        return grpc::StatusCode::FAILED_PRECONDITION;
      }
      return grpc::StatusCode::OK;
    }
    if (!response.has_data() || !response.has_eof() || response.data().size() > count ||
        response.data().size() > data_capacity) {
      *nfs_status = 5;
      return grpc::StatusCode::FAILED_PRECONDITION;
    }
    if (!response.data().empty()) {
      std::memcpy(data, response.data().data(), response.data().size());
    }
    *bytes_read = response.data().size();
    *eof = response.eof() ? 1 : 0;
    return grpc::StatusCode::OK;
  } catch (...) {
    *bytes_read = 0;
    *eof = 0;
    *nfs_status = 5;
    return grpc::StatusCode::INTERNAL;
  }
}

extern "C" int ozone_grpc_getattr(struct ozone_grpc_client *client,
    const uint8_t export_uuid[16], const char *kerberos_principal,
    size_t kerberos_principal_size, uint32_t protection, const uint8_t *handle,
    size_t handle_size, struct ozone_nfs_attributes *attributes, uint32_t *nfs_status) {
  if (attributes) std::memset(attributes, 0, sizeof(*attributes));
  if (nfs_status) *nfs_status = 5;
  if (!client || !export_uuid || !valid_caller(kerberos_principal, kerberos_principal_size, protection) ||
      !handle || handle_size != OZONE_NFS_HANDLE_BYTES || !attributes || !nfs_status) {
    return grpc::StatusCode::INVALID_ARGUMENT;
  }
  try {
    ozone::nfs::v1::GetattrRequest request;
    set_context(request.mutable_context(), export_uuid, kerberos_principal, kerberos_principal_size, protection);
    request.set_handle(handle, handle_size);
    ozone::nfs::v1::GetattrResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
        std::chrono::milliseconds(client->timeout_millis));
    grpc::Status status = client->stub->Getattr(&context, request, &response);
    if (!status.ok()) return grpc_status(status, nfs_status);
    if (!response.IsInitialized()) return grpc::StatusCode::FAILED_PRECONDITION;
    *nfs_status = response.result().nfs_status();
    if (*nfs_status != 0) {
      return response.has_attributes() ? grpc::StatusCode::FAILED_PRECONDITION : grpc::StatusCode::OK;
    }
    if (!response.has_attributes() || !copy_attributes(response.attributes(), attributes)) {
      *nfs_status = 5;
      return grpc::StatusCode::FAILED_PRECONDITION;
    }
    return grpc::StatusCode::OK;
  } catch (...) {
    std::memset(attributes, 0, sizeof(*attributes));
    *nfs_status = 5;
    return grpc::StatusCode::INTERNAL;
  }
}

extern "C" int ozone_grpc_lookup(struct ozone_grpc_client *client,
    const uint8_t export_uuid[16], const char *kerberos_principal,
    size_t kerberos_principal_size, uint32_t protection, const uint8_t *parent_handle,
    size_t parent_handle_size, const uint8_t *name, size_t name_size,
    struct ozone_nfs_attributes *attributes, uint32_t *nfs_status) {
  if (attributes) std::memset(attributes, 0, sizeof(*attributes));
  if (nfs_status) *nfs_status = 5;
  if (!client || !export_uuid || !valid_caller(kerberos_principal, kerberos_principal_size, protection) ||
      !parent_handle || parent_handle_size != OZONE_NFS_HANDLE_BYTES || !name || !name_size || name_size > 255 ||
      std::memchr(name, 0, name_size) || std::memchr(name, '/', name_size) || !attributes || !nfs_status) {
    return grpc::StatusCode::INVALID_ARGUMENT;
  }
  try {
    ozone::nfs::v1::LookupRequest request;
    set_context(request.mutable_context(), export_uuid, kerberos_principal, kerberos_principal_size, protection);
    request.set_parent_handle(parent_handle, parent_handle_size);
    request.set_name(reinterpret_cast<const char *>(name), name_size);
    ozone::nfs::v1::LookupResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
        std::chrono::milliseconds(client->timeout_millis));
    grpc::Status status = client->stub->Lookup(&context, request, &response);
    if (!status.ok()) return grpc_status(status, nfs_status);
    if (!response.IsInitialized()) return grpc::StatusCode::FAILED_PRECONDITION;
    *nfs_status = response.result().nfs_status();
    if (*nfs_status != 0) {
      return response.has_attributes() ? grpc::StatusCode::FAILED_PRECONDITION : grpc::StatusCode::OK;
    }
    if (!response.has_attributes() || !copy_attributes(response.attributes(), attributes)) {
      *nfs_status = 5;
      return grpc::StatusCode::FAILED_PRECONDITION;
    }
    return grpc::StatusCode::OK;
  } catch (...) {
    std::memset(attributes, 0, sizeof(*attributes));
    *nfs_status = 5;
    return grpc::StatusCode::INTERNAL;
  }
}

extern "C" int ozone_grpc_create(struct ozone_grpc_client *client,
    const uint8_t export_uuid[16], const char *kerberos_principal,
    size_t kerberos_principal_size, uint32_t protection, const uint8_t *parent_handle,
    size_t parent_handle_size, const uint8_t *name, size_t name_size,
    struct ozone_nfs_attributes *attributes, char *transfer_id, size_t transfer_id_capacity,
    size_t *transfer_id_size, uint32_t *nfs_status) {
  if (attributes) std::memset(attributes, 0, sizeof(*attributes));
  if (transfer_id && transfer_id_capacity) std::memset(transfer_id, 0, transfer_id_capacity);
  if (transfer_id_size) *transfer_id_size = 0;
  if (nfs_status) *nfs_status = 5;
  if (!client || !export_uuid || !valid_caller(kerberos_principal, kerberos_principal_size, protection) ||
      !parent_handle || parent_handle_size != OZONE_NFS_HANDLE_BYTES || !name || !name_size ||
      name_size > OZONE_NFS_MAX_NAME_BYTES || std::memchr(name, 0, name_size) ||
      std::memchr(name, '/', name_size) || !attributes || !transfer_id ||
      transfer_id_capacity < OZONE_NFS_MAX_TRANSFER_ID_BYTES || !transfer_id_size || !nfs_status) {
    return grpc::StatusCode::INVALID_ARGUMENT;
  }
  try {
    ozone::nfs::v1::CreateRequest request;
    set_context(request.mutable_context(), export_uuid, kerberos_principal, kerberos_principal_size, protection);
    request.set_parent_handle(parent_handle, parent_handle_size);
    request.set_name(reinterpret_cast<const char *>(name), name_size);
    ozone::nfs::v1::CreateResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
        std::chrono::milliseconds(client->timeout_millis));
    grpc::Status status = client->stub->Create(&context, request, &response);
    if (!status.ok()) return grpc_status(status, nfs_status);
    if (!response.IsInitialized()) return grpc::StatusCode::FAILED_PRECONDITION;
    *nfs_status = response.result().nfs_status();
    if (*nfs_status != 0) {
      return response.has_attributes() || response.has_transfer_id()
          ? grpc::StatusCode::FAILED_PRECONDITION : grpc::StatusCode::OK;
    }
    if (!response.has_attributes() || !response.has_transfer_id() ||
        response.transfer_id().size() != OZONE_NFS_MAX_TRANSFER_ID_BYTES ||
        !copy_attributes(response.attributes(), attributes)) {
      std::memset(attributes, 0, sizeof(*attributes));
      *nfs_status = 5;
      return grpc::StatusCode::FAILED_PRECONDITION;
    }
    std::memcpy(transfer_id, response.transfer_id().data(), response.transfer_id().size());
    *transfer_id_size = response.transfer_id().size();
    return grpc::StatusCode::OK;
  } catch (...) {
    std::memset(attributes, 0, sizeof(*attributes));
    std::memset(transfer_id, 0, transfer_id_capacity);
    *transfer_id_size = 0;
    *nfs_status = 5;
    return grpc::StatusCode::INTERNAL;
  }
}

extern "C" int ozone_grpc_mkdir(struct ozone_grpc_client *client,
    const uint8_t export_uuid[16], const char *kerberos_principal,
    size_t kerberos_principal_size, uint32_t protection, const uint8_t *parent_handle,
    size_t parent_handle_size, const uint8_t *name, size_t name_size,
    struct ozone_nfs_attributes *attributes, uint32_t *nfs_status) {
  if (attributes) std::memset(attributes, 0, sizeof(*attributes));
  if (nfs_status) *nfs_status = 5;
  if (!client || !export_uuid || !valid_caller(kerberos_principal, kerberos_principal_size, protection) ||
      !parent_handle || parent_handle_size != OZONE_NFS_HANDLE_BYTES || !name || !name_size ||
      name_size > OZONE_NFS_MAX_NAME_BYTES || std::memchr(name, 0, name_size) ||
      std::memchr(name, '/', name_size) || !attributes || !nfs_status) {
    return grpc::StatusCode::INVALID_ARGUMENT;
  }
  try {
    ozone::nfs::v1::MkdirRequest request;
    set_context(request.mutable_context(), export_uuid, kerberos_principal, kerberos_principal_size, protection);
    request.set_parent_handle(parent_handle, parent_handle_size);
    request.set_name(reinterpret_cast<const char *>(name), name_size);
    ozone::nfs::v1::MkdirResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
        std::chrono::milliseconds(client->timeout_millis));
    grpc::Status status = client->stub->Mkdir(&context, request, &response);
    if (!status.ok()) return grpc_status(status, nfs_status);
    if (!response.IsInitialized()) return grpc::StatusCode::FAILED_PRECONDITION;
    *nfs_status = response.result().nfs_status();
    if (*nfs_status != 0) {
      return response.has_attributes() ? grpc::StatusCode::FAILED_PRECONDITION : grpc::StatusCode::OK;
    }
    if (!response.has_attributes() || !copy_attributes(response.attributes(), attributes) ||
        attributes->directory != 1) {
      std::memset(attributes, 0, sizeof(*attributes));
      *nfs_status = 5;
      return grpc::StatusCode::FAILED_PRECONDITION;
    }
    return grpc::StatusCode::OK;
  } catch (...) {
    std::memset(attributes, 0, sizeof(*attributes));
    *nfs_status = 5;
    return grpc::StatusCode::INTERNAL;
  }
}

extern "C" int ozone_grpc_remove(struct ozone_grpc_client *client,
    const uint8_t export_uuid[16], const char *kerberos_principal,
    size_t kerberos_principal_size, uint32_t protection, const uint8_t *parent_handle,
    size_t parent_handle_size, const uint8_t *object_handle, size_t object_handle_size,
    const uint8_t *name, size_t name_size, uint32_t *nfs_status) {
  if (nfs_status) *nfs_status = 5;
  if (!client || !export_uuid || !valid_caller(kerberos_principal, kerberos_principal_size, protection) ||
      !parent_handle || parent_handle_size != OZONE_NFS_HANDLE_BYTES || !object_handle ||
      object_handle_size != OZONE_NFS_HANDLE_BYTES || !name || !name_size ||
      name_size > OZONE_NFS_MAX_NAME_BYTES || std::memchr(name, 0, name_size) ||
      std::memchr(name, '/', name_size) || !nfs_status) {
    return grpc::StatusCode::INVALID_ARGUMENT;
  }
  try {
    ozone::nfs::v1::RemoveRequest request;
    set_context(request.mutable_context(), export_uuid, kerberos_principal, kerberos_principal_size, protection);
    request.set_parent_handle(parent_handle, parent_handle_size);
    request.set_object_handle(object_handle, object_handle_size);
    request.set_name(reinterpret_cast<const char *>(name), name_size);
    ozone::nfs::v1::RemoveResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
        std::chrono::milliseconds(client->timeout_millis));
    grpc::Status status = client->stub->Remove(&context, request, &response);
    if (!status.ok()) return grpc_status(status, nfs_status);
    if (!response.IsInitialized()) return grpc::StatusCode::FAILED_PRECONDITION;
    *nfs_status = response.result().nfs_status();
    return grpc::StatusCode::OK;
  } catch (...) {
    *nfs_status = 5;
    return grpc::StatusCode::INTERNAL;
  }
}

extern "C" int ozone_grpc_rename(struct ozone_grpc_client *client,
    const uint8_t export_uuid[16], const char *kerberos_principal,
    size_t kerberos_principal_size, uint32_t protection, const uint8_t *object_handle,
    size_t object_handle_size, const uint8_t *old_parent_handle, size_t old_parent_handle_size,
    const uint8_t *old_name, size_t old_name_size, const uint8_t *new_parent_handle,
    size_t new_parent_handle_size, const uint8_t *new_name, size_t new_name_size,
    uint32_t *nfs_status) {
  if (nfs_status) *nfs_status = 5;
  if (!client || !export_uuid || !valid_caller(kerberos_principal, kerberos_principal_size, protection) ||
      !object_handle || object_handle_size != OZONE_NFS_HANDLE_BYTES || !old_parent_handle ||
      old_parent_handle_size != OZONE_NFS_HANDLE_BYTES || !new_parent_handle ||
      new_parent_handle_size != OZONE_NFS_HANDLE_BYTES || !old_name || !old_name_size ||
      old_name_size > OZONE_NFS_MAX_NAME_BYTES || std::memchr(old_name, 0, old_name_size) ||
      std::memchr(old_name, '/', old_name_size) || !new_name || !new_name_size ||
      new_name_size > OZONE_NFS_MAX_NAME_BYTES || std::memchr(new_name, 0, new_name_size) ||
      std::memchr(new_name, '/', new_name_size) || !nfs_status) {
    return grpc::StatusCode::INVALID_ARGUMENT;
  }
  try {
    ozone::nfs::v1::RenameRequest request;
    set_context(request.mutable_context(), export_uuid, kerberos_principal, kerberos_principal_size, protection);
    request.set_object_handle(object_handle, object_handle_size);
    request.set_old_parent_handle(old_parent_handle, old_parent_handle_size);
    request.set_old_name(reinterpret_cast<const char *>(old_name), old_name_size);
    request.set_new_parent_handle(new_parent_handle, new_parent_handle_size);
    request.set_new_name(reinterpret_cast<const char *>(new_name), new_name_size);
    ozone::nfs::v1::RenameResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
        std::chrono::milliseconds(client->timeout_millis));
    grpc::Status status = client->stub->Rename(&context, request, &response);
    if (!status.ok()) return grpc_status(status, nfs_status);
    if (!response.IsInitialized()) return grpc::StatusCode::FAILED_PRECONDITION;
    *nfs_status = response.result().nfs_status();
    return grpc::StatusCode::OK;
  } catch (...) {
    *nfs_status = 5;
    return grpc::StatusCode::INTERNAL;
  }
}

extern "C" int ozone_grpc_write(struct ozone_grpc_client *client,
    const uint8_t export_uuid[16], const char *kerberos_principal,
    size_t kerberos_principal_size, uint32_t protection, const uint8_t *handle,
    size_t handle_size, const char *transfer_id, size_t transfer_id_size, uint64_t offset,
    const uint8_t *data, size_t data_size, uint32_t *count, uint32_t *committed,
    uint32_t *nfs_status) {
  if (count) *count = 0;
  if (committed) *committed = 0;
  if (nfs_status) *nfs_status = 5;
  if (!client || !export_uuid || !valid_caller(kerberos_principal, kerberos_principal_size, protection) ||
      !handle || handle_size != OZONE_NFS_HANDLE_BYTES ||
      (transfer_id_size != 0 && transfer_id_size != OZONE_NFS_MAX_TRANSFER_ID_BYTES) ||
      (transfer_id_size != 0 && (!transfer_id || std::memchr(transfer_id, 0, transfer_id_size))) ||
      !data || !data_size || data_size > OZONE_NFS_MAX_READ_BYTES || !count || !committed || !nfs_status) {
    return grpc::StatusCode::INVALID_ARGUMENT;
  }
  try {
    ozone::nfs::v1::WriteRequest request;
    set_context(request.mutable_context(), export_uuid, kerberos_principal, kerberos_principal_size, protection);
    request.set_handle(handle, handle_size);
    if (transfer_id_size == 0) {
      request.set_transfer_id("");
    } else {
      request.set_transfer_id(transfer_id, transfer_id_size);
    }
    request.set_offset(offset);
    request.set_data(data, data_size);
    ozone::nfs::v1::WriteResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
        std::chrono::milliseconds(client->timeout_millis));
    grpc::Status status = client->stub->Write(&context, request, &response);
    if (!status.ok()) return grpc_status(status, nfs_status);
    if (!response.IsInitialized()) return grpc::StatusCode::FAILED_PRECONDITION;
    *nfs_status = response.result().nfs_status();
    if (*nfs_status != 0) {
      return response.has_count() || response.has_committed()
          ? grpc::StatusCode::FAILED_PRECONDITION : grpc::StatusCode::OK;
    }
    if (!response.has_count() || !response.has_committed() || response.count() > data_size ||
        response.committed() < ozone::nfs::v1::WriteResponse_Stability_UNSTABLE ||
        response.committed() > ozone::nfs::v1::WriteResponse_Stability_FILE_SYNC) {
      *nfs_status = 5;
      return grpc::StatusCode::FAILED_PRECONDITION;
    }
    *count = response.count();
    *committed = static_cast<uint32_t>(response.committed());
    return grpc::StatusCode::OK;
  } catch (...) {
    *count = 0;
    *committed = 0;
    *nfs_status = 5;
    return grpc::StatusCode::INTERNAL;
  }
}

extern "C" int ozone_grpc_commit(struct ozone_grpc_client *client,
    const uint8_t export_uuid[16], const char *kerberos_principal,
    size_t kerberos_principal_size, uint32_t protection, const uint8_t *handle,
    size_t handle_size, uint32_t finalize, uint32_t *nfs_status) {
  if (nfs_status) *nfs_status = 5;
  if (!client || !export_uuid || !valid_caller(kerberos_principal, kerberos_principal_size, protection) ||
      !handle || handle_size != OZONE_NFS_HANDLE_BYTES || finalize > 1 || !nfs_status) {
    return grpc::StatusCode::INVALID_ARGUMENT;
  }
  try {
    ozone::nfs::v1::CommitRequest request;
    set_context(request.mutable_context(), export_uuid, kerberos_principal, kerberos_principal_size, protection);
    request.set_handle(handle, handle_size);
    request.set_finalize(finalize != 0);
    ozone::nfs::v1::CommitResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
        std::chrono::milliseconds(client->timeout_millis));
    grpc::Status status = client->stub->Commit(&context, request, &response);
    if (!status.ok()) return grpc_status(status, nfs_status);
    if (!response.IsInitialized()) return grpc::StatusCode::FAILED_PRECONDITION;
    *nfs_status = response.result().nfs_status();
    return grpc::StatusCode::OK;
  } catch (...) {
    *nfs_status = 5;
    return grpc::StatusCode::INTERNAL;
  }
}

extern "C" int ozone_grpc_readdir(struct ozone_grpc_client *client,
    const uint8_t export_uuid[16], const char *kerberos_principal,
    size_t kerberos_principal_size, uint32_t protection, const uint8_t *directory_handle,
    size_t directory_handle_size, uint32_t max_entries, const uint8_t *cookie,
    size_t cookie_size, const uint8_t *verifier, size_t verifier_size,
    struct ozone_nfs_directory_entry *entries, size_t entry_capacity, size_t *entries_read,
    uint8_t response_verifier[OZONE_NFS_VERIFIER_BYTES], uint32_t *eof, uint32_t *nfs_status) {
  if (entries_read) *entries_read = 0;
  if (response_verifier) std::memset(response_verifier, 0, OZONE_NFS_VERIFIER_BYTES);
  if (eof) *eof = 0;
  if (nfs_status) *nfs_status = 5;
  if (entries && entry_capacity <= OZONE_NFS_MAX_DIRECTORY_ENTRIES) {
    std::memset(entries, 0, entry_capacity * sizeof(*entries));
  }
  const bool has_cookie = cookie_size != 0;
  if (!client || !export_uuid || !valid_caller(kerberos_principal, kerberos_principal_size, protection) ||
      !directory_handle || directory_handle_size != OZONE_NFS_HANDLE_BYTES || !max_entries ||
      max_entries > OZONE_NFS_MAX_DIRECTORY_ENTRIES || (verifier_size != 0 && !has_cookie) ||
      (has_cookie && (cookie_size != OZONE_NFS_VERIFIER_BYTES || !cookie ||
          (verifier_size == 0 ? verifier != nullptr :
              (!verifier || verifier_size != OZONE_NFS_VERIFIER_BYTES)))) ||
      (!has_cookie && (cookie || verifier || verifier_size)) || !entries ||
      entry_capacity < max_entries || entry_capacity > OZONE_NFS_MAX_DIRECTORY_ENTRIES ||
      !entries_read || !response_verifier || !eof || !nfs_status) {
    return grpc::StatusCode::INVALID_ARGUMENT;
  }
  try {
    ozone::nfs::v1::ReaddirRequest request;
    set_context(request.mutable_context(), export_uuid, kerberos_principal, kerberos_principal_size, protection);
    request.set_directory_handle(directory_handle, directory_handle_size);
    request.set_max_entries(max_entries);
    if (has_cookie) {
      request.set_cookie(cookie, cookie_size);
    }
    if (verifier_size != 0) {
      request.set_verifier(verifier, verifier_size);
    }
    ozone::nfs::v1::ReaddirResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
        std::chrono::milliseconds(client->timeout_millis));
    grpc::Status status = client->stub->Readdir(&context, request, &response);
    if (!status.ok()) return grpc_status(status, nfs_status);
    if (!response.IsInitialized()) return grpc::StatusCode::FAILED_PRECONDITION;
    *nfs_status = response.result().nfs_status();
    if (*nfs_status != 0) {
      if (response.entries_size() || response.has_verifier() || response.has_eof()) {
        *nfs_status = 5;
        return grpc::StatusCode::FAILED_PRECONDITION;
      }
      return grpc::StatusCode::OK;
    }
    if (response.entries_size() > static_cast<int>(max_entries) || !response.has_verifier() ||
        response.verifier().size() != OZONE_NFS_VERIFIER_BYTES || !response.has_eof()) {
      *nfs_status = 5;
      return grpc::StatusCode::FAILED_PRECONDITION;
    }
    for (int i = 0; i < response.entries_size(); ++i) {
      const auto &source = response.entries(i);
      auto &target = entries[i];
      if (source.name().empty() || source.name().size() > OZONE_NFS_MAX_NAME_BYTES ||
          source.cookie().size() != OZONE_NFS_VERIFIER_BYTES ||
          !copy_attributes(source.attributes(), &target.attributes)) {
        std::memset(entries, 0, entry_capacity * sizeof(*entries));
        *nfs_status = 5;
        return grpc::StatusCode::FAILED_PRECONDITION;
      }
      target.name_size = source.name().size();
      std::memcpy(target.name, source.name().data(), target.name_size);
      target.cookie_size = source.cookie().size();
      std::memcpy(target.cookie, source.cookie().data(), target.cookie_size);
    }
    std::memcpy(response_verifier, response.verifier().data(), OZONE_NFS_VERIFIER_BYTES);
    *entries_read = response.entries_size();
    *eof = response.eof() ? 1 : 0;
    return grpc::StatusCode::OK;
  } catch (...) {
    std::memset(entries, 0, entry_capacity * sizeof(*entries));
    *entries_read = 0;
    std::memset(response_verifier, 0, OZONE_NFS_VERIFIER_BYTES);
    *eof = 0;
    *nfs_status = 5;
    return grpc::StatusCode::INTERNAL;
  }
}
