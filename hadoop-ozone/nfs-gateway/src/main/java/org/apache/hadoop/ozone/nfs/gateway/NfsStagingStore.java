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

package org.apache.hadoop.ozone.nfs.gateway;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.security.UserGroupInformation;

/** Stores a retry-safe NFS range in the private Ozone staging bucket. */
final class NfsStagingStore {
  private final String volumeName;
  private final String bucketName;
  private final UserGroupInformation gatewayUser;
  private final ConfigurationSource configuration;
  private final ClientFactory clientFactory;

  NfsStagingStore(String volumeName, String bucketName, UserGroupInformation gatewayUser,
      ConfigurationSource configuration, ClientFactory clientFactory) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.gatewayUser = gatewayUser;
    this.configuration = configuration;
    this.clientFactory = clientFactory;
  }

  StagedRange stage(String exportUuid, String transferId, long offset, byte[] data) throws IOException {
    if (volumeName == null || volumeName.isEmpty() || bucketName == null || bucketName.isEmpty()) {
      throw new IOException("NFS durable write staging bucket is not configured");
    }
    UUID.fromString(exportUuid);
    UUID.fromString(transferId);
    if (offset < 0 || data == null || data.length == 0 || data.length > NfsObjectReader.MAX_READ_BYTES) {
      throw new IOException("NFS staged range is outside the supported bounds");
    }
    if (offset > Long.MAX_VALUE - data.length) {
      throw new IOException("NFS staged range is outside the supported bounds");
    }
    byte[] digest = sha256(data);
    String key = exportUuid + "/" + transferId + "/" + String.format(java.util.Locale.ROOT, "%020d", offset)
        + "/" + toHex(digest);
    try {
      gatewayUser.doAs((PrivilegedExceptionAction<Void>) () -> {
        RpcClient client = clientFactory.create();
        try {
          OzoneBucket bucket = client.getBucketDetails(volumeName, bucketName);
          if (bucket.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
            throw new IOException("NFS staging bucket must use the native FSO layout");
          }
          try (OzoneOutputStream output = client.createFile(volumeName, bucketName, key, data.length,
              NfsReplicationConfig.forBucket(bucket, configuration), false, true)) {
            output.write(data);
          } catch (OMException e) {
            if (e.getResult() != OMException.ResultCodes.KEY_ALREADY_EXISTS || !matches(client, key, data, digest)) {
              throw e;
            }
          }
        } finally {
          client.close();
        }
        return null;
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while storing an NFS range", e);
    }
    return new StagedRange(volumeName + "/" + bucketName + "/" + key, offset, data.length,
        ByteString.copyFrom(digest));
  }

  private boolean matches(RpcClient client, String key, byte[] data, byte[] digest) throws IOException {
    try (InputStream input = client.readFile(volumeName, bucketName, key)) {
      ByteArrayOutputStream existing = new ByteArrayOutputStream(data.length);
      byte[] buffer = new byte[Math.min(data.length, 8192)];
      int count;
      while ((count = input.read(buffer)) >= 0) {
        if (existing.size() + count > data.length) {
          return false;
        }
        existing.write(buffer, 0, count);
      }
      byte[] existingData = existing.toByteArray();
      return MessageDigest.isEqual(digest, sha256(existingData)) && MessageDigest.isEqual(data, existingData);
    }
  }

  byte[] read(String qualifiedKey) throws IOException {
    String prefix = volumeName + "/" + bucketName + "/";
    if (qualifiedKey == null || !qualifiedKey.startsWith(prefix)) {
      throw new IOException("NFS staging locator does not match the configured staging bucket");
    }
    String key = qualifiedKey.substring(prefix.length());
    try {
      return gatewayUser.doAs((PrivilegedExceptionAction<byte[]>) () -> {
        RpcClient client = clientFactory.create();
        try (InputStream input = client.readFile(volumeName, bucketName, key)) {
          ByteArrayOutputStream data = new ByteArrayOutputStream();
          byte[] buffer = new byte[8192];
          int count;
          while ((count = input.read(buffer)) >= 0) {
            if (data.size() + count > NfsObjectReader.MAX_READ_BYTES) {
              throw new IOException("NFS staged range exceeds the backend limit");
            }
            data.write(buffer, 0, count);
          }
          return data.toByteArray();
        } finally {
          client.close();
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while reading an NFS staged range", e);
    }
  }

  void delete(String qualifiedKey) throws IOException {
    String prefix = volumeName + "/" + bucketName + "/";
    if (qualifiedKey == null || !qualifiedKey.startsWith(prefix)) {
      throw new IOException("NFS staging locator does not match the configured staging bucket");
    }
    String key = qualifiedKey.substring(prefix.length());
    try {
      gatewayUser.doAs((PrivilegedExceptionAction<Void>) () -> {
        RpcClient client = clientFactory.create();
        try {
          try {
            client.deleteKey(volumeName, bucketName, key, false);
          } catch (OMException e) {
            if (e.getResult() != OMException.ResultCodes.KEY_NOT_FOUND) {
              throw e;
            }
          }
        } finally {
          client.close();
        }
        return null;
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while deleting an NFS staging object", e);
    }
  }

  void deleteTransfer(String exportUuid, String transferId) throws IOException {
    if (volumeName == null || volumeName.isEmpty() || bucketName == null || bucketName.isEmpty()) {
      throw new IOException("NFS durable write staging bucket is not configured");
    }
    UUID.fromString(exportUuid);
    UUID.fromString(transferId);
    String key = exportUuid + "/" + transferId;
    try {
      gatewayUser.doAs((PrivilegedExceptionAction<Void>) () -> {
        RpcClient client = clientFactory.create();
        try {
          OzoneBucket bucket = client.getBucketDetails(volumeName, bucketName);
          try {
            bucket.deleteDirectory(key, true);
          } catch (OMException e) {
            if (e.getResult() != OMException.ResultCodes.KEY_NOT_FOUND) {
              throw e;
            }
          }
        } finally {
          client.close();
        }
        return null;
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while deleting an NFS staged transfer", e);
    }
  }

  private static byte[] sha256(byte[] data) throws IOException {
    try {
      return MessageDigest.getInstance("SHA-256").digest(data);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("SHA-256 is unavailable", e);
    }
  }

  private static String toHex(byte[] data) {
    char[] digits = "0123456789abcdef".toCharArray();
    char[] result = new char[data.length * 2];
    for (int i = 0; i < data.length; i++) {
      result[i * 2] = digits[(data[i] >>> 4) & 0x0f];
      result[i * 2 + 1] = digits[data[i] & 0x0f];
    }
    return new String(result);
  }

  interface ClientFactory {
    RpcClient create() throws IOException;
  }

  static final class StagedRange {
    private final String qualifiedKey;
    private final long offset;
    private final long length;
    private final ByteString sha256;

    StagedRange(String qualifiedKey, long length, ByteString sha256) {
      this(qualifiedKey, 0, length, sha256);
    }

    StagedRange(String qualifiedKey, long offset, long length, ByteString sha256) {
      this.qualifiedKey = qualifiedKey;
      this.offset = offset;
      this.length = length;
      this.sha256 = sha256;
    }

    String getQualifiedKey() {
      return qualifiedKey;
    }

    long getOffset() {
      return offset;
    }

    long getLength() {
      return length;
    }

    ByteString getSha256() {
      return sha256;
    }
  }
}
