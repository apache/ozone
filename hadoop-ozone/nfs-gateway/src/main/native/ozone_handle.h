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

#ifndef OZONE_NFS_HANDLE_H
#define OZONE_NFS_HANDLE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define OZONE_NFS_HANDLE_LENGTH 48

/* Values are part of the versioned backend payload, not enum ordinals. */
enum ozone_nfs_kind {
  OZONE_NFS_BUCKET = 1,
  OZONE_NFS_DIRECTORY = 2,
  OZONE_NFS_FILE = 3
};

struct ozone_nfs_handle {
  uint8_t export_id[16];
  uint64_t volume_id;
  uint64_t bucket_id;
  uint64_t object_id;
  enum ozone_nfs_kind kind;
};

/* Return 0 on success or EINVAL for a malformed/unsupported payload.
 * This codec does not resolve objects or authorize access.
 */
int ozone_nfs_handle_encode(const struct ozone_nfs_handle *handle, uint8_t *payload, size_t length);
int ozone_nfs_handle_decode(const uint8_t *payload, size_t length, struct ozone_nfs_handle *handle);

#ifdef __cplusplus
}
#endif

#endif
