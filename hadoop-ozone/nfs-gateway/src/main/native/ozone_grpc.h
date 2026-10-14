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

#ifndef OZONE_NFS_GRPC_H
#define OZONE_NFS_GRPC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ozone_grpc_client;

#define OZONE_NFS_HANDLE_BYTES 48
#define OZONE_NFS_MAX_READ_BYTES (1024u * 1024u)
#define OZONE_NFS_MAX_OWNER_BYTES 1024u
#define OZONE_NFS_MAX_DIRECTORY_ENTRIES 256u
#define OZONE_NFS_MAX_NAME_BYTES 255u
#define OZONE_NFS_MAX_TRANSFER_ID_BYTES 36u
#define OZONE_NFS_VERIFIER_BYTES 8u

struct ozone_nfs_attributes {
  uint8_t handle[OZONE_NFS_HANDLE_BYTES];
  uint32_t directory;
  uint64_t object_id;
  uint64_t change_id;
  uint64_t size;
  uint64_t modification_time_millis;
  uint64_t creation_time_millis;
  uint8_t owner[OZONE_NFS_MAX_OWNER_BYTES];
  size_t owner_size;
};

struct ozone_nfs_directory_entry {
  uint8_t name[OZONE_NFS_MAX_NAME_BYTES];
  size_t name_size;
  struct ozone_nfs_attributes attributes;
  uint8_t cookie[OZONE_NFS_VERIFIER_BYTES];
  size_t cookie_size;
};

/* All PEM inputs are mandatory. No plaintext or anonymous-client fallback. */
struct ozone_grpc_client *ozone_grpc_client_create(const char *address,
    const char *trusted_ca_pem, const char *client_certificate_pem,
    const char *client_key_pem, uint32_t timeout_millis);
void ozone_grpc_client_destroy(struct ozone_grpc_client *client);

/* Return a gRPC transport status separately from the explicit NFS result. */
int ozone_grpc_capabilities(struct ozone_grpc_client *client,
    const uint8_t export_uuid[16], uint32_t version,
    uint32_t *nfs_status, uint64_t *operations);

int ozone_grpc_getattr(struct ozone_grpc_client *client, const uint8_t export_uuid[16],
    const char *kerberos_principal, size_t kerberos_principal_size, uint32_t protection,
    const uint8_t *handle, size_t handle_size, struct ozone_nfs_attributes *attributes,
    uint32_t *nfs_status);

int ozone_grpc_lookup(struct ozone_grpc_client *client, const uint8_t export_uuid[16],
    const char *kerberos_principal, size_t kerberos_principal_size, uint32_t protection,
    const uint8_t *parent_handle, size_t parent_handle_size, const uint8_t *name,
    size_t name_size, struct ozone_nfs_attributes *attributes, uint32_t *nfs_status);

int ozone_grpc_create(struct ozone_grpc_client *client, const uint8_t export_uuid[16],
    const char *kerberos_principal, size_t kerberos_principal_size, uint32_t protection,
    const uint8_t *parent_handle, size_t parent_handle_size, const uint8_t *name,
    size_t name_size, struct ozone_nfs_attributes *attributes, char *transfer_id,
    size_t transfer_id_capacity, size_t *transfer_id_size, uint32_t *nfs_status);

int ozone_grpc_mkdir(struct ozone_grpc_client *client, const uint8_t export_uuid[16],
    const char *kerberos_principal, size_t kerberos_principal_size, uint32_t protection,
    const uint8_t *parent_handle, size_t parent_handle_size, const uint8_t *name,
    size_t name_size, struct ozone_nfs_attributes *attributes, uint32_t *nfs_status);

int ozone_grpc_remove(struct ozone_grpc_client *client, const uint8_t export_uuid[16],
    const char *kerberos_principal, size_t kerberos_principal_size, uint32_t protection,
    const uint8_t *parent_handle, size_t parent_handle_size, const uint8_t *object_handle,
    size_t object_handle_size, const uint8_t *name, size_t name_size, uint32_t *nfs_status);

int ozone_grpc_rename(struct ozone_grpc_client *client, const uint8_t export_uuid[16],
    const char *kerberos_principal, size_t kerberos_principal_size, uint32_t protection,
    const uint8_t *object_handle, size_t object_handle_size, const uint8_t *old_parent_handle,
    size_t old_parent_handle_size, const uint8_t *old_name, size_t old_name_size,
    const uint8_t *new_parent_handle, size_t new_parent_handle_size,
    const uint8_t *new_name, size_t new_name_size, uint32_t *nfs_status);

int ozone_grpc_write(struct ozone_grpc_client *client, const uint8_t export_uuid[16],
    const char *kerberos_principal, size_t kerberos_principal_size, uint32_t protection,
    const uint8_t *handle, size_t handle_size, const char *transfer_id, size_t transfer_id_size,
    uint64_t offset, const uint8_t *data, size_t data_size, uint32_t *count,
    uint32_t *committed, uint32_t *nfs_status);

int ozone_grpc_commit(struct ozone_grpc_client *client, const uint8_t export_uuid[16],
    const char *kerberos_principal, size_t kerberos_principal_size, uint32_t protection,
    const uint8_t *handle, size_t handle_size, uint32_t finalize, uint32_t *nfs_status);

int ozone_grpc_readdir(struct ozone_grpc_client *client, const uint8_t export_uuid[16],
    const char *kerberos_principal, size_t kerberos_principal_size, uint32_t protection,
    const uint8_t *directory_handle, size_t directory_handle_size, uint32_t max_entries,
    const uint8_t *cookie, size_t cookie_size, const uint8_t *verifier, size_t verifier_size,
    struct ozone_nfs_directory_entry *entries, size_t entry_capacity, size_t *entries_read,
    uint8_t response_verifier[OZONE_NFS_VERIFIER_BYTES], uint32_t *eof, uint32_t *nfs_status);

/* Read only a bounded range from a caller authenticated by Ganesha RPCSEC_GSS. */
int ozone_grpc_read(struct ozone_grpc_client *client, const uint8_t export_uuid[16],
    const char *kerberos_principal, size_t kerberos_principal_size, uint32_t protection,
    const uint8_t *handle, size_t handle_size, uint64_t offset, uint32_t count,
    uint8_t *data, size_t data_capacity, size_t *bytes_read,
    uint32_t *eof, uint32_t *nfs_status);

#ifdef __cplusplus
}
#endif
#endif
