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
#include "ozone_handle.h"
#include <algorithm>
#include <fstream>
#include <array>
#include <cstring>
#include <vector>
#include <iostream>
#include <iterator>
#include <string>
#include <stdexcept>
#include <cstdlib>

static std::string read_file(const char *path) {
  std::ifstream input(path);
  if (!input) throw std::runtime_error("Cannot read TLS fixture");
  return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
}

int main(int argc, char **argv) {
  if (argc != 8) return 2;
  try {
    std::string ca = read_file(argv[2]);
    std::string certificate = read_file(argv[3]);
    std::string key = read_file(argv[4]);
    uint8_t export_uuid[16] = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                             0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff};
    const std::string mode(argv[5]);
    if (ozone_grpc_client_create(argv[1], nullptr, certificate.c_str(), key.c_str(), 3000) ||
        ozone_grpc_client_create(argv[1], ca.c_str(), nullptr, key.c_str(), 3000) ||
        ozone_grpc_client_create(argv[1], ca.c_str(), certificate.c_str(), nullptr, 3000) ||
        ozone_grpc_client_create(argv[1], ca.c_str(), certificate.c_str(), key.c_str(), 0)) return 5;
    auto *client = ozone_grpc_client_create(argv[1], ca.c_str(), certificate.c_str(), key.c_str(), 3000);
    if (!client) return 3;
    uint32_t nfs_status = 0;
    uint64_t operations = UINT64_MAX;
    size_t bytes_read = 0;
    uint32_t eof = 0;
    int status;
    if (mode.compare(0, 8, "getattr-") == 0 || mode == "getattr") {
      ozone_nfs_handle decoded{};
      uint8_t handle[OZONE_NFS_HANDLE_BYTES];
      decoded.export_id[0] = 0;
      std::copy(export_uuid, export_uuid + 16, decoded.export_id);
      decoded.volume_id = 1;
      decoded.bucket_id = 2;
      decoded.object_id = mode == "getattr-stale" ? 5 : 3;
      decoded.kind = OZONE_NFS_FILE;
      if (ozone_nfs_handle_encode(&decoded, handle, sizeof(handle)) != 0) return 6;
      const std::string principal = mode == "getattr-denied" ? "bob@EXAMPLE.COM" : "alice@EXAMPLE.COM";
      ozone_nfs_attributes attributes{};
      status = ozone_grpc_getattr(client, export_uuid, principal.data(), principal.size(), 2,
          handle, sizeof(handle), &attributes, &nfs_status);
      bool result_ok = status == std::atoi(argv[6]) && nfs_status == static_cast<uint32_t>(std::atoi(argv[7]));
      if (nfs_status == 0 && status == 0) {
        result_ok = result_ok && attributes.object_id == 3 && attributes.size == 19 &&
            attributes.change_id == 9 && attributes.modification_time_millis == 13 &&
            attributes.creation_time_millis == 11 && attributes.directory == 0 && attributes.owner_size == 5 &&
            std::string(reinterpret_cast<const char *>(attributes.owner), attributes.owner_size) == "alice" &&
            std::memcmp(attributes.handle, handle, sizeof(handle)) == 0;
      } else {
        result_ok = result_ok && attributes.object_id == 0 && attributes.owner_size == 0;
      }
      std::cout << "transport=" << status << " nfs=" << nfs_status << " object=" << attributes.object_id << std::endl;
      ozone_grpc_client_destroy(client);
      return result_ok ? 0 : 1;
    }
    if (mode.compare(0, 7, "lookup-") == 0 || mode == "lookup") {
      ozone_nfs_handle decoded{};
      uint8_t parent_handle[OZONE_NFS_HANDLE_BYTES];
      std::copy(export_uuid, export_uuid + 16, decoded.export_id);
      decoded.volume_id = 1;
      decoded.bucket_id = 2;
      decoded.object_id = 2;
      decoded.kind = OZONE_NFS_DIRECTORY;
      if (ozone_nfs_handle_encode(&decoded, parent_handle, sizeof(parent_handle)) != 0) return 6;
      const std::string principal = mode == "lookup-denied" ? "bob@EXAMPLE.COM" : "alice@EXAMPLE.COM";
      const std::string name = mode == "lookup-invalid-name" ? "a/b"
          : mode == "lookup-unicode" ? "café 文件" : "child";
      ozone_nfs_attributes attributes{};
      status = ozone_grpc_lookup(client, export_uuid, principal.data(), principal.size(), 2,
          parent_handle, sizeof(parent_handle), reinterpret_cast<const uint8_t *>(name.data()), name.size(),
          &attributes, &nfs_status);
      bool result_ok = status == std::atoi(argv[6]) && nfs_status == static_cast<uint32_t>(std::atoi(argv[7]));
      if (nfs_status == 0 && status == 0) {
        result_ok = result_ok && attributes.object_id == 3 && attributes.size == 19 &&
            attributes.directory == 0 && attributes.owner_size == 5;
      } else {
        result_ok = result_ok && attributes.object_id == 0 && attributes.owner_size == 0;
      }
      std::cout << "transport=" << status << " nfs=" << nfs_status << " object=" << attributes.object_id << std::endl;
      ozone_grpc_client_destroy(client);
      return result_ok ? 0 : 1;
    }
    if (mode == "mkdir" || mode == "remove" || mode == "rename") {
      ozone_nfs_handle parent{};
      uint8_t parent_handle[OZONE_NFS_HANDLE_BYTES];
      std::copy(export_uuid, export_uuid + 16, parent.export_id);
      parent.volume_id = 1;
      parent.bucket_id = 2;
      parent.object_id = 2;
      parent.kind = OZONE_NFS_BUCKET;
      if (ozone_nfs_handle_encode(&parent, parent_handle, sizeof(parent_handle)) != 0) return 6;
      const std::string principal = "alice@EXAMPLE.COM";
      const std::string name = mode == "mkdir" ? "new-directory" : "child";
      if (mode == "mkdir") {
        ozone_nfs_attributes attributes{};
        status = ozone_grpc_mkdir(client, export_uuid, principal.data(), principal.size(), 2,
            parent_handle, sizeof(parent_handle), reinterpret_cast<const uint8_t *>(name.data()), name.size(),
            &attributes, &nfs_status);
        const bool result_ok = status == std::atoi(argv[6]) &&
            nfs_status == static_cast<uint32_t>(std::atoi(argv[7])) && attributes.object_id == 5 &&
            attributes.directory == 1;
        std::cout << "transport=" << status << " nfs=" << nfs_status << " object=" << attributes.object_id
                  << std::endl;
        ozone_grpc_client_destroy(client);
        return result_ok ? 0 : 1;
      }
      ozone_nfs_handle object{};
      uint8_t object_handle[OZONE_NFS_HANDLE_BYTES];
      std::copy(export_uuid, export_uuid + 16, object.export_id);
      object.volume_id = 1;
      object.bucket_id = 2;
      object.object_id = 3;
      object.kind = OZONE_NFS_FILE;
      if (ozone_nfs_handle_encode(&object, object_handle, sizeof(object_handle)) != 0) return 6;
      if (mode == "remove") {
        status = ozone_grpc_remove(client, export_uuid, principal.data(), principal.size(), 2,
            parent_handle, sizeof(parent_handle), object_handle, sizeof(object_handle),
            reinterpret_cast<const uint8_t *>(name.data()), name.size(), &nfs_status);
      } else {
        status = ozone_grpc_rename(client, export_uuid, principal.data(), principal.size(), 2,
            object_handle, sizeof(object_handle), parent_handle, sizeof(parent_handle),
            reinterpret_cast<const uint8_t *>(name.data()), name.size(), parent_handle, sizeof(parent_handle),
            reinterpret_cast<const uint8_t *>(name.data()), name.size(), &nfs_status);
      }
      const bool result_ok = status == std::atoi(argv[6]) &&
          nfs_status == static_cast<uint32_t>(std::atoi(argv[7]));
      std::cout << "transport=" << status << " nfs=" << nfs_status << std::endl;
      ozone_grpc_client_destroy(client);
      return result_ok ? 0 : 1;
    }
    if (mode == "create") {
      ozone_nfs_handle decoded{};
      uint8_t parent_handle[OZONE_NFS_HANDLE_BYTES];
      std::copy(export_uuid, export_uuid + 16, decoded.export_id);
      decoded.volume_id = 1;
      decoded.bucket_id = 2;
      decoded.object_id = 2;
      decoded.kind = OZONE_NFS_BUCKET;
      if (ozone_nfs_handle_encode(&decoded, parent_handle, sizeof(parent_handle)) != 0) return 6;
      const std::string principal = "alice@EXAMPLE.COM";
      const std::string name = "new-file";
      ozone_nfs_attributes attributes{};
      char transfer_id[OZONE_NFS_MAX_TRANSFER_ID_BYTES] = {};
      size_t transfer_id_size = 0;
      status = ozone_grpc_create(client, export_uuid, principal.data(), principal.size(), 2,
          parent_handle, sizeof(parent_handle), reinterpret_cast<const uint8_t *>(name.data()), name.size(),
          &attributes, transfer_id, sizeof(transfer_id), &transfer_id_size, &nfs_status);
      const bool result_ok = status == std::atoi(argv[6]) &&
          nfs_status == static_cast<uint32_t>(std::atoi(argv[7])) && attributes.object_id == 4 &&
          attributes.directory == 0 && attributes.size == 0 && transfer_id_size == 36 &&
          std::string(transfer_id, transfer_id_size).size() == 36;
      std::cout << "transport=" << status << " nfs=" << nfs_status << " object=" << attributes.object_id
                << " transfer=" << transfer_id_size << std::endl;
      ozone_grpc_client_destroy(client);
      return result_ok ? 0 : 1;
    }
    if (mode == "write" || mode == "write-recover") {
      ozone_nfs_handle decoded{};
      uint8_t handle[OZONE_NFS_HANDLE_BYTES];
      std::copy(export_uuid, export_uuid + 16, decoded.export_id);
      decoded.volume_id = 1;
      decoded.bucket_id = 2;
      decoded.object_id = 4;
      decoded.kind = OZONE_NFS_FILE;
      if (ozone_nfs_handle_encode(&decoded, handle, sizeof(handle)) != 0) return 6;
      const std::string principal = "alice@EXAMPLE.COM";
      const std::string transfer_id = "00112233-4455-6677-8899-aabbccddeeff";
      const bool recover = mode == "write-recover";
      const uint8_t data[] = {1, 2, 3};
      uint32_t count = 0, committed = 0;
      status = ozone_grpc_write(client, export_uuid, principal.data(), principal.size(), 2, handle, sizeof(handle),
          recover ? nullptr : transfer_id.data(), recover ? 0 : transfer_id.size(), 4, data, sizeof(data),
          &count, &committed, &nfs_status);
      const bool result_ok = status == std::atoi(argv[6]) &&
          nfs_status == static_cast<uint32_t>(std::atoi(argv[7])) && count == sizeof(data) && committed == 0;
      std::cout << "transport=" << status << " nfs=" << nfs_status << " count=" << count
                << " committed=" << committed << std::endl;
      ozone_grpc_client_destroy(client);
      return result_ok ? 0 : 1;
    }
    if (mode == "commit" || mode == "close") {
      ozone_nfs_handle decoded{};
      uint8_t handle[OZONE_NFS_HANDLE_BYTES];
      std::copy(export_uuid, export_uuid + 16, decoded.export_id);
      decoded.volume_id = 1;
      decoded.bucket_id = 2;
      decoded.object_id = 4;
      decoded.kind = OZONE_NFS_FILE;
      if (ozone_nfs_handle_encode(&decoded, handle, sizeof(handle)) != 0) return 6;
      const std::string principal = "alice@EXAMPLE.COM";
      uint32_t nfs_status = 0;
      status = ozone_grpc_commit(client, export_uuid, principal.data(), principal.size(), 2, handle,
          sizeof(handle), mode == "close" ? 1 : 0, &nfs_status);
      const bool result_ok = status == std::atoi(argv[6]) &&
          nfs_status == static_cast<uint32_t>(std::atoi(argv[7]));
      std::cout << "transport=" << status << " nfs=" << nfs_status << std::endl;
      ozone_grpc_client_destroy(client);
      return result_ok ? 0 : 1;
    }
    if (mode.compare(0, 8, "readdir-") == 0 || mode == "readdir") {
      ozone_nfs_handle decoded{};
      uint8_t directory_handle[OZONE_NFS_HANDLE_BYTES];
      std::copy(export_uuid, export_uuid + 16, decoded.export_id);
      decoded.volume_id = 1;
      decoded.bucket_id = 2;
      decoded.object_id = 2;
      decoded.kind = OZONE_NFS_DIRECTORY;
      if (ozone_nfs_handle_encode(&decoded, directory_handle, sizeof(directory_handle)) != 0) return 6;
      const std::string principal = mode == "readdir-denied" ? "bob@EXAMPLE.COM" : "alice@EXAMPLE.COM";
      const bool has_cookie = mode != "readdir";
      const bool has_verifier = has_cookie && mode != "readdir-no-verifier";
      uint8_t cookie[OZONE_NFS_VERIFIER_BYTES]{};
      cookie[7] = 6;
      uint8_t verifier[OZONE_NFS_VERIFIER_BYTES] = {};
      if (mode == "readdir-next" || mode == "readdir-invalid-verifier") verifier[7] =
          mode == "readdir-next" ? 7 : 8;
      ozone_nfs_directory_entry entries[2]{};
      size_t entries_read = 0;
      uint8_t response_verifier[OZONE_NFS_VERIFIER_BYTES]{};
      uint32_t eof = 0;
      const uint32_t max_entries = mode == "readdir-invalid-count" ? 257 : 2;
      const size_t cookie_size = mode == "readdir-invalid-cookie-size" ? sizeof(cookie) - 1 : sizeof(cookie);
      status = ozone_grpc_readdir(client, export_uuid, principal.data(), principal.size(), 2,
          directory_handle, sizeof(directory_handle), max_entries,
          has_cookie ? cookie : nullptr, has_cookie ? cookie_size : 0,
          has_verifier ? verifier : nullptr, has_verifier ? sizeof(verifier) : 0, entries, 2,
          &entries_read, response_verifier, &eof, &nfs_status);
      bool result_ok = status == std::atoi(argv[6]) && nfs_status == static_cast<uint32_t>(std::atoi(argv[7]));
      if (nfs_status == 0 && status == 0) {
        const bool first_page = !has_cookie;
        result_ok = result_ok && entries_read == (first_page ? 1u : 0u) && eof == 1 && response_verifier[7] == 7;
        if (first_page) {
          result_ok = result_ok && entries[0].name_size == 5 &&
              std::string(reinterpret_cast<const char *>(entries[0].name), entries[0].name_size) == "child" &&
              entries[0].attributes.object_id == 3 && entries[0].cookie_size == OZONE_NFS_VERIFIER_BYTES &&
              entries[0].cookie[7] == 6;
        }
      } else {
        result_ok = result_ok && entries_read == 0 && eof == 0 && response_verifier[7] == 0 &&
            entries[0].name_size == 0;
      }
      std::cout << "transport=" << status << " nfs=" << nfs_status << " entries=" << entries_read
                << " eof=" << eof << std::endl;
      ozone_grpc_client_destroy(client);
      return result_ok ? 0 : 1;
    }
    if (mode == "read" || mode.compare(0, 5, "read-") == 0) {
      uint8_t handle[OZONE_NFS_HANDLE_BYTES] = {
          0x4f, 0x5a, 0x4e, 0x46, 1, 3, 0, 0,
          0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
          0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
          0, 0, 0, 0, 0, 0, 0, 1,
          0, 0, 0, 0, 0, 0, 0, 2,
          0, 0, 0, 0, 0, 0, 0, 3};
      const std::string principal = mode == "read-unauthenticated" ? "mallory@EXAMPLE.COM" :
          (mode == "read-embedded-nul" ? std::string("alice\0@EXAMPLE.COM", 18) :
           (mode == "read-denied" ? "bob@EXAMPLE.COM" : "alice@EXAMPLE.COM"));
      uint64_t offset = mode == "read-eof" ? UINT64_MAX : (mode == "read-full" ? 0 : 1);
      uint32_t count = mode == "read-full" ? 64 : 4;
      if (mode == "read-stale") handle[47] = 5;
      if (mode == "read-badhandle") handle[0] = 0;
      if (mode == "read-wrong-export") export_uuid[15] ^= 1;
      std::vector<uint8_t> data(OZONE_NFS_MAX_READ_BYTES, 0xa5);
      const char *principal_data = principal.data();
      if (mode == "read-invalid-args") {
        status = ozone_grpc_read(client, export_uuid, principal_data, principal.size(), 2, handle, sizeof(handle),
            offset, OZONE_NFS_MAX_READ_BYTES + 1, data.data(), data.size(), &bytes_read, &eof, &nfs_status);
      } else if (mode == "read-invalid-principal") {
        status = ozone_grpc_read(client, export_uuid, principal_data, 1025, 2, handle, sizeof(handle), offset,
            count, data.data(), data.size(), &bytes_read, &eof, &nfs_status);
      } else if (mode == "read-small-buffer") {
        status = ozone_grpc_read(client, export_uuid, principal_data, principal.size(), 2, handle, sizeof(handle),
            offset, count, data.data(), count - 1, &bytes_read, &eof, &nfs_status);
      } else if (mode == "read-null-principal") {
        status = ozone_grpc_read(client, export_uuid, nullptr, 0, 2, handle, sizeof(handle), offset, count,
            data.data(), data.size(), &bytes_read, &eof, &nfs_status);
      } else {
        status = ozone_grpc_read(client, export_uuid, principal_data, principal.size(), 2, handle, sizeof(handle),
            offset, count, data.data(), data.size(), &bytes_read, &eof, &nfs_status);
      }
      const std::string received(data.begin(), data.begin() + bytes_read);
      const std::string expected = mode == "read" ? "ativ" : "native READ interop";
      bool result_ok = status == std::atoi(argv[6]) && nfs_status == static_cast<uint32_t>(std::atoi(argv[7]));
      if (nfs_status == 0 && status == 0) {
        result_ok = result_ok && ((mode == "read-eof") ? (bytes_read == 0 && eof == 1) :
            ((mode == "read" || mode == "read-full") ? (received == expected && eof == (mode == "read-full")) :
             (bytes_read == 0 && eof == 0)));
      } else {
        result_ok = result_ok && bytes_read == 0 && eof == 0 && data.front() == 0xa5;
      }
      std::cout << "transport=" << status << " nfs=" << nfs_status << " bytes=" << bytes_read
                << " eof=" << eof << std::endl;
      ozone_grpc_client_destroy(client);
      return result_ok ? 0 : 1;
    }
    if (mode == "wrong-export") export_uuid[15] ^= 1;
    status = ozone_grpc_capabilities(client, export_uuid, mode == "unsupported-version" ? 2 : 1,
        &nfs_status, &operations);
    ozone_grpc_client_destroy(client);
    std::cout << "transport=" << status << " nfs=" << nfs_status << " operations=" << operations << std::endl;
    const bool supported = status == 0 && nfs_status == 0 && mode != "wrong-export" &&
        mode != "unsupported-version";
    const uint64_t expected_operations = supported ? ((UINT64_C(1) << 1) | (UINT64_C(1) << 2) |
        (UINT64_C(1) << 3) | (UINT64_C(1) << 4) | (UINT64_C(1) << 5) |
        (UINT64_C(1) << 6) | (UINT64_C(1) << 7) | (UINT64_C(1) << 8)) : 0;
    return status == std::atoi(argv[6]) && nfs_status == static_cast<uint32_t>(std::atoi(argv[7]))
        && operations == expected_operations ? 0 : 1;
  } catch (...) {
    return 4;
  }
}
