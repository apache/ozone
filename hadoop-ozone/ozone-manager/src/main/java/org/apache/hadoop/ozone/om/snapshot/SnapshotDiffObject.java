/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.snapshot;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hdds.utils.db.Codec;

import java.io.IOException;

/**
 * SnapshotDiffObject to keep objectId, key's oldName and newName.
 */
public class SnapshotDiffObject {

  private static final Codec<SnapshotDiffObject> CODEC =
      new SnapshotDiffObjectCodec();

  public static Codec<SnapshotDiffObject> getCodec() {
    return CODEC;
  }

  private long objectId;
  private String oldKeyName;
  private String newKeyName;

  // Default constructor for Jackson Serializer.
  public SnapshotDiffObject() {

  }

  public SnapshotDiffObject(long objectId,
                            String oldKeyName,
                            String newKeyName) {
    this.objectId = objectId;
    this.oldKeyName = oldKeyName;
    this.newKeyName = newKeyName;
  }

  public long getObjectId() {
    return objectId;
  }

  public String getOldKeyName() {
    return oldKeyName;
  }

  public String getNewKeyName() {
    return newKeyName;
  }

  // For Jackson Serializer.
  public void setObjectId(long objectId) {
    this.objectId = objectId;
  }

  // For Jackson Serializer.
  public void setOldKeyName(String oldKeyName) {
    this.oldKeyName = oldKeyName;
  }

  // For Jackson Serializer.
  public void setNewKeyName(String newKeyName) {
    this.newKeyName = newKeyName;
  }

  /**
   * Builder for SnapshotDiffObject.
   */
  public static class SnapshotDiffObjectBuilder {
    private final long objectId;
    private String oldKeyName;
    private String newKeyName;

    public SnapshotDiffObjectBuilder(long objectID) {
      this.objectId = objectID;
    }

    public static SnapshotDiffObjectBuilder newBuilder(long objectID) {
      return new SnapshotDiffObjectBuilder(objectID);
    }

    public SnapshotDiffObjectBuilder withOldKeyName(String keyName) {
      this.oldKeyName = keyName;
      return this;
    }

    public SnapshotDiffObjectBuilder withNewKeyName(String keyName) {
      this.newKeyName = keyName;
      return this;
    }

    public SnapshotDiffObject build() {
      return new SnapshotDiffObject(objectId, oldKeyName, newKeyName);
    }
  }


  /**
   * Codec to encode KeyObject as byte array.
   */
  private static final class SnapshotDiffObjectCodec
      implements Codec<SnapshotDiffObject> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public byte[] toPersistedFormat(SnapshotDiffObject object)
        throws IOException {
      return MAPPER.writeValueAsBytes(object);
    }

    @Override
    public SnapshotDiffObject fromPersistedFormat(byte[] rawData)
        throws IOException {
      return MAPPER.readValue(rawData, SnapshotDiffObject.class);
    }

    @Override
    public SnapshotDiffObject copyObject(SnapshotDiffObject object) {
      // Note: Not really a "copy".
      return object;
    }
  }
}
