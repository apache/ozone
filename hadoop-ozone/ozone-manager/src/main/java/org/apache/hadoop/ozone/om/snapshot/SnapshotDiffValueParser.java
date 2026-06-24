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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest.HSYNC_FIELD_NUMBER;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.KeyValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DirectoryInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;

/**
 * Parses snapshot diff values without full deserialization.
 */
public final class SnapshotDiffValueParser {
  private static final int INT_BYTES = 4;
  private static final int LONG_BYTES = 8;
  private static final String DIGEST_ALGORITHM = "SHA-256";

  private SnapshotDiffValueParser() {
  }

  public static ParsedRequiredInfo parseKeyInfoRequiredFields(byte[] value, boolean includeUpdateId)
      throws IOException {
    CodedInputStream input = CodedInputStream.newInstance(value);
    long updateId = 0L;
    long objectId = 0L;
    long parentId = 0L;
    boolean hasUpdateId = false;
    String keyName = null;

    int tag;
    while ((tag = input.readTag()) != 0) {
      int fieldNumber = WireFormat.getTagFieldNumber(tag);
      switch (fieldNumber) {
      case KeyInfo.KEYNAME_FIELD_NUMBER:
        keyName = input.readString();
        break;
      case KeyInfo.OBJECTID_FIELD_NUMBER:
        objectId = input.readUInt64();
        break;
      case KeyInfo.UPDATEID_FIELD_NUMBER:
        if (includeUpdateId) {
          updateId = input.readUInt64();
          hasUpdateId = true;
        } else {
          input.skipField(tag);
        }
        break;
      case KeyInfo.PARENTID_FIELD_NUMBER:
        parentId = input.readUInt64();
        break;
      default:
        input.skipField(tag);
        break;
      }
    }

    return new ParsedRequiredInfo(updateId, hasUpdateId, objectId, parentId, keyName);
  }

  public static byte[] computeKeyInfoCompareSignature(byte[] value) throws IOException {
    CodedInputStream input = CodedInputStream.newInstance(value);
    MessageDigest digest = newDigest();
    boolean hasHsyncMetadata = false;
    int keyLocationListCount = 0;
    ByteString latestKeyLocationList = null;

    int tag;
    while ((tag = input.readTag()) != 0) {
      int fieldNumber = WireFormat.getTagFieldNumber(tag);
      switch (fieldNumber) {
      case KeyInfo.DATASIZE_FIELD_NUMBER:
        updateDigestWithLong(digest, fieldNumber, input.readUInt64());
        break;
      case KeyInfo.KEYLOCATIONLIST_FIELD_NUMBER:
        latestKeyLocationList = input.readBytes();
        keyLocationListCount++;
        break;
      case KeyInfo.METADATA_FIELD_NUMBER:
        MetadataEntry metadataEntry = parseMetadataEntry(input.readBytes().toByteArray());
        if (metadataEntry != null) {
          updateDigestWithRawBytes(digest, fieldNumber, metadataEntry.getDigest());
          if (metadataEntry.hasHsync()) {
            hasHsyncMetadata = true;
          }
        }
        break;
      case KeyInfo.FILECHECKSUM_FIELD_NUMBER:
      case KeyInfo.ACLS_FIELD_NUMBER:
      case KeyInfo.TAGS_FIELD_NUMBER:
        updateDigestWithBytes(digest, fieldNumber, input.readBytes());
        break;
      default:
        input.skipField(tag);
        break;
      }
    }

    if (latestKeyLocationList != null) {
      updateDigestWithBytes(digest, KeyInfo.KEYLOCATIONLIST_FIELD_NUMBER, latestKeyLocationList);
    }
    updateDigestWithBoolean(digest, HSYNC_FIELD_NUMBER, hasHsyncMetadata);
    updateDigestWithInt(digest, keyLocationListCount);

    return digest.digest();
  }

  public static ParsedRequiredInfo parseDirectoryInfoRequiredFields(byte[] value, boolean includeUpdateId)
      throws IOException {
    CodedInputStream input = CodedInputStream.newInstance(value);
    long updateId = 0L;
    long objectId = 0L;
    long parentId = 0L;
    boolean hasUpdateId = false;
    String name = null;

    int tag;
    while ((tag = input.readTag()) != 0) {
      int fieldNumber = WireFormat.getTagFieldNumber(tag);
      switch (fieldNumber) {
      case DirectoryInfo.NAME_FIELD_NUMBER:
        name = input.readString();
        break;
      case DirectoryInfo.OBJECTID_FIELD_NUMBER:
        objectId = input.readUInt64();
        break;
      case DirectoryInfo.UPDATEID_FIELD_NUMBER:
        if (includeUpdateId) {
          updateId = input.readUInt64();
          hasUpdateId = true;
        } else {
          input.skipField(tag);
        }
        break;
      case DirectoryInfo.PARENTID_FIELD_NUMBER:
        parentId = input.readUInt64();
        break;
      default:
        input.skipField(tag);
        break;
      }
    }

    return new ParsedRequiredInfo(updateId, hasUpdateId, objectId, parentId, name);
  }

  public static byte[] computeDirectoryInfoCompareSignature(byte[] value) throws IOException {
    CodedInputStream input = CodedInputStream.newInstance(value);
    MessageDigest digest = newDigest();

    int tag;
    while ((tag = input.readTag()) != 0) {
      int fieldNumber = WireFormat.getTagFieldNumber(tag);
      switch (fieldNumber) {
      case DirectoryInfo.METADATA_FIELD_NUMBER:
        MetadataEntry metadataEntry = parseMetadataEntry(input.readBytes().toByteArray());
        if (metadataEntry != null) {
          updateDigestWithRawBytes(digest, fieldNumber, metadataEntry.getDigest());
        }
        break;
      case DirectoryInfo.ACLS_FIELD_NUMBER:
        updateDigestWithBytes(digest, fieldNumber, input.readBytes());
        break;
      default:
        input.skipField(tag);
        break;
      }
    }

    return digest.digest();
  }

  private static void updateDigestWithLong(MessageDigest digest, int fieldNumber, long value) {
    updateDigestWithInt(digest, fieldNumber);
    byte[] buffer = new byte[LONG_BYTES];
    for (int i = LONG_BYTES - 1; i >= 0; i--) {
      buffer[i] = (byte) (value & 0xFFL);
      value >>>= 8;
    }
    digest.update(buffer);
  }

  private static void updateDigestWithBytes(MessageDigest digest, int fieldNumber, ByteString value) {
    updateDigestWithInt(digest, fieldNumber);
    updateDigestWithInt(digest, value.size());
    digest.update(value.toByteArray());
  }

  private static void updateTaggedString(MessageDigest digest, int fieldNumber, String value) {
    updateDigestWithInt(digest, fieldNumber);
    if (value == null) {
      updateDigestWithInt(digest, 0);
      return;
    }
    byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    updateDigestWithInt(digest, bytes.length);
    digest.update(bytes);
  }

  private static void updateDigestWithBoolean(MessageDigest digest, int fieldNumber, boolean value) {
    updateDigestWithInt(digest, fieldNumber);
    updateDigestWithInt(digest, value ? 1 : 0);
  }

  private static void updateDigestWithRawBytes(MessageDigest digest, int fieldNumber, byte[] value) {
    updateDigestWithInt(digest, fieldNumber);
    updateDigestWithInt(digest, value.length);
    digest.update(value);
  }

  private static void updateDigestWithInt(MessageDigest digest, int value) {
    byte[] buffer = new byte[INT_BYTES];
    for (int i = INT_BYTES - 1; i >= 0; i--) {
      buffer[i] = (byte) (value & 0xFF);
      value >>>= 8;
    }
    digest.update(buffer);
  }

  private static MessageDigest newDigest() {
    try {
      return MessageDigest.getInstance(DIGEST_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }

  private static MetadataEntry parseMetadataEntry(byte[] metadataBytes) throws IOException {
    if (metadataBytes == null || metadataBytes.length == 0) {
      return null;
    }
    CodedInputStream input = CodedInputStream.newInstance(metadataBytes);
    String key = null;
    String value = null;
    while (!input.isAtEnd()) {
      int tag = input.readTag();
      if (tag == 0) {
        break;
      }
      int fieldNumber = WireFormat.getTagFieldNumber(tag);
      switch (fieldNumber) {
      case KeyValue.KEY_FIELD_NUMBER:
        key = input.readString();
        break;
      case KeyValue.VALUE_FIELD_NUMBER:
        value = input.readString();
        break;
      default:
        input.skipField(tag);
        break;
      }
    }
    MessageDigest entryDigest = newDigest();
    updateTaggedString(entryDigest, KeyValue.KEY_FIELD_NUMBER, key);
    updateTaggedString(entryDigest, KeyValue.VALUE_FIELD_NUMBER, value);
    return new MetadataEntry(entryDigest.digest(), OzoneConsts.HSYNC_CLIENT_ID.equals(key));
  }

  private static final class MetadataEntry {
    private final byte[] digest;
    private final boolean hasHsync;

    private MetadataEntry(byte[] digest, boolean hasHsync) {
      this.digest = digest;
      this.hasHsync = hasHsync;
    }

    private byte[] getDigest() {
      return digest;
    }

    private boolean hasHsync() {
      return hasHsync;
    }
  }

  /**
   * Parsed fields shared by key and directory entries.
   * Holds IDs and name with optional updateID when requested.
   */
  public static final class ParsedRequiredInfo {
    private final long updateId;
    private final boolean hasUpdateId;
    private final long objectId;
    private final long parentId;
    private final String name;

    private ParsedRequiredInfo(long updateId, boolean hasUpdateId, long objectId, long parentId, String name) {
      this.updateId = updateId;
      this.hasUpdateId = hasUpdateId;
      this.objectId = objectId;
      this.parentId = parentId;
      this.name = name;
    }

    public long getUpdateId() {
      return updateId;
    }

    public boolean hasUpdateId() {
      return hasUpdateId;
    }

    public long getObjectId() {
      return objectId;
    }

    public long getParentId() {
      return parentId;
    }

    public String getName() {
      return name;
    }
  }
}
