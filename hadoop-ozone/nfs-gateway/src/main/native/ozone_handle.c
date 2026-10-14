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

#include "ozone_handle.h"

#include <errno.h>
#include <string.h>

static int valid_kind(unsigned int kind) {
  return kind == OZONE_NFS_BUCKET || kind == OZONE_NFS_DIRECTORY || kind == OZONE_NFS_FILE;
}

static void encode_id(uint8_t *payload, uint64_t id) {
  unsigned int i;
  for (i = 0; i < 8; i++) {
    payload[7 - i] = (uint8_t) (id >> (i * 8));
  }
}

static uint64_t decode_id(const uint8_t *payload) {
  uint64_t id = 0;
  unsigned int i;
  for (i = 0; i < 8; i++) {
    id = (id << 8) | payload[i];
  }
  return id;
}

int ozone_nfs_handle_encode(const struct ozone_nfs_handle *handle, uint8_t *payload, size_t length) {
  if (handle == NULL || payload == NULL || length != OZONE_NFS_HANDLE_LENGTH || !valid_kind(handle->kind)) {
    return EINVAL;
  }
  memcpy(payload, "OZNF", 4);
  payload[4] = 1;
  payload[5] = (uint8_t) handle->kind;
  payload[6] = 0;
  payload[7] = 0;
  memcpy(payload + 8, handle->export_id, 16);
  encode_id(payload + 24, handle->volume_id);
  encode_id(payload + 32, handle->bucket_id);
  encode_id(payload + 40, handle->object_id);
  return 0;
}

int ozone_nfs_handle_decode(const uint8_t *payload, size_t length, struct ozone_nfs_handle *handle) {
  if (payload == NULL || handle == NULL || length != OZONE_NFS_HANDLE_LENGTH
      || memcmp(payload, "OZNF", 4) != 0 || payload[4] != 1 || !valid_kind(payload[5])
      || payload[6] != 0 || payload[7] != 0) {
    return EINVAL;
  }
  memcpy(handle->export_id, payload + 8, 16);
  handle->volume_id = decode_id(payload + 24);
  handle->bucket_id = decode_id(payload + 32);
  handle->object_id = decode_id(payload + 40);
  handle->kind = (enum ozone_nfs_kind) payload[5];
  return 0;
}
