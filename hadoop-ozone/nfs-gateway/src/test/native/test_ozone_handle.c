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
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(condition) do { \
  if (!(condition)) { \
    fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition); \
    return 1; \
  } \
} while (0)

static int decode_hex(const char *hex, uint8_t *bytes, size_t count) {
  size_t i;
  if (strlen(hex) != count * 2) {
    return 1;
  }
  for (i = 0; i < count; i++) {
    char pair[3] = {hex[i * 2], hex[i * 2 + 1], 0};
    char *end;
    unsigned long value = strtoul(pair, &end, 16);
    if (*end != 0 || end == pair || value > 255) {
      return 1;
    }
    bytes[i] = (uint8_t) value;
  }
  return 0;
}

int main(int argc, char **argv) {
  FILE *vectors;
  char line[512];
  unsigned int count = 0;
  CHECK(argc == 2);
  vectors = fopen(argv[1], "r");
  CHECK(vectors != NULL);
  while (fgets(line, sizeof(line), vectors) != NULL) {
    char uuid[37], uuid_hex[33], kind[16], hex[97];
    struct ozone_nfs_handle original, decoded;
    uint8_t payload[OZONE_NFS_HANDLE_LENGTH], expected[OZONE_NFS_HANDLE_LENGTH];
    size_t i, j = 0;
    static const unsigned int malformed_offsets[] = {0, 4, 5, 6, 7};
    if (line[0] == '#' || line[0] == '\n') {
      continue;
    }
    CHECK(sscanf(line, "%36s %" SCNx64 " %" SCNx64 " %" SCNx64 " %15s %96s", uuid,
        &original.volume_id, &original.bucket_id, &original.object_id, kind, hex) == 6);
    for (i = 0; i < strlen(uuid); i++) {
      if (uuid[i] != '-') {
        CHECK(j < sizeof(uuid_hex) - 1);
        uuid_hex[j++] = uuid[i];
      }
    }
    uuid_hex[j] = 0;
    CHECK(decode_hex(uuid_hex, original.export_id, 16) == 0);
    if (strcmp(kind, "BUCKET") == 0) {
      original.kind = OZONE_NFS_BUCKET;
    } else if (strcmp(kind, "DIRECTORY") == 0) {
      original.kind = OZONE_NFS_DIRECTORY;
    } else {
      CHECK(strcmp(kind, "FILE") == 0);
      original.kind = OZONE_NFS_FILE;
    }
    CHECK(decode_hex(hex, expected, sizeof(expected)) == 0);
    CHECK(ozone_nfs_handle_encode(&original, payload, sizeof(payload)) == 0);
    CHECK(memcmp(payload, expected, sizeof(payload)) == 0);
    CHECK(ozone_nfs_handle_decode(expected, sizeof(expected), &decoded) == 0);
    CHECK(decoded.kind == original.kind);
    CHECK(decoded.volume_id == original.volume_id);
    CHECK(decoded.bucket_id == original.bucket_id);
    CHECK(decoded.object_id == original.object_id);
    CHECK(memcmp(decoded.export_id, original.export_id, 16) == 0);
    for (i = 0; i < sizeof(malformed_offsets) / sizeof(malformed_offsets[0]); i++) {
      memcpy(payload, expected, sizeof(payload));
      payload[malformed_offsets[i]] = 255;
      CHECK(ozone_nfs_handle_decode(payload, sizeof(payload), &decoded) == EINVAL);
    }
    /* Exercise every unsupported header value, including zero and valid kind codes. */
    for (i = 0; i < 8; i++) {
      unsigned int value;
      for (value = 0; value <= 255; value++) {
        int valid = value == expected[i];
        if (i == 5) {
          valid = value == OZONE_NFS_BUCKET || value == OZONE_NFS_DIRECTORY || value == OZONE_NFS_FILE;
        }
        memcpy(payload, expected, sizeof(payload));
        payload[i] = (uint8_t) value;
        CHECK(ozone_nfs_handle_decode(payload, sizeof(payload), &decoded) == (valid ? 0 : EINVAL));
      }
    }
    for (i = 0; i < sizeof(expected); i++) {
      CHECK(ozone_nfs_handle_decode(expected, i, &decoded) == EINVAL);
      CHECK(ozone_nfs_handle_encode(&original, payload, i) == EINVAL);
    }
    CHECK(ozone_nfs_handle_decode(expected, sizeof(expected) - 1, &decoded) == EINVAL);
    CHECK(ozone_nfs_handle_decode(expected, sizeof(expected) + 1, &decoded) == EINVAL);
    CHECK(ozone_nfs_handle_decode(NULL, sizeof(expected), &decoded) == EINVAL);
    CHECK(ozone_nfs_handle_decode(expected, sizeof(expected), NULL) == EINVAL);
    CHECK(ozone_nfs_handle_encode(&original, payload, sizeof(payload) - 1) == EINVAL);
    CHECK(ozone_nfs_handle_encode(NULL, payload, sizeof(payload)) == EINVAL);
    CHECK(ozone_nfs_handle_encode(&original, NULL, sizeof(payload)) == EINVAL);
    original.kind = (enum ozone_nfs_kind) 255;
    CHECK(ozone_nfs_handle_encode(&original, payload, sizeof(payload)) == EINVAL);
    count++;
  }
  CHECK(!ferror(vectors));
  CHECK(fclose(vectors) == 0);
  CHECK(count == 4);
  printf("Verified %u shared Java/C handle vectors and malformed payloads\n", count);
  return 0;
}
