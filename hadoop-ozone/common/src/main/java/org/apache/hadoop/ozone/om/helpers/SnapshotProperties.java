package org.apache.hadoop.ozone.om.helpers;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.util.Objects;

/**
 * This class is used for storing info related to Snapshots.
 *
 * Each snapshot created has an associated SnapshotInfo entry
 * containing the snapshotId, snapshot path,
 * snapshot checkpoint directory, previous snapshotId
 * for the snapshot path & global amongst other necessary fields.
 */
public final class SnapshotProperties {
  private static final Codec<SnapshotProperties> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(
          OzoneManagerProtocolProtos.SnapshotProperties.getDefaultInstance()),
      SnapshotProperties::getFromProtobuf,
      SnapshotProperties::getProtobuf);

  public static Codec<SnapshotProperties> getCodec() {
    return CODEC;
  }

  private boolean sstFiltered;
  private String snapshotTableKey;
  private boolean snapshotPurged;

  public void setSnapshotPurged(boolean snapshotPurged) {
    this.snapshotPurged = snapshotPurged;
  }

  private SnapshotProperties(Builder b) {
    this.sstFiltered = b.sstFiltered;
    this.snapshotTableKey = b.snapshotTableKey;
    this.snapshotPurged = b.snapshotPurged;
  }

  public boolean isSstFiltered() {
    return sstFiltered;
  }

  public String getSnapshotTableKey() {
    return snapshotTableKey;
  }

  public boolean isSnapshotPurged() {
    return snapshotPurged;
  }

  public void setSstFiltered(boolean sstFiltered) {
    this.sstFiltered = sstFiltered;
  }

  public static SnapshotProperties.Builder
      newBuilder() {
    return new SnapshotProperties.Builder();
  }

  public SnapshotProperties.Builder toBuilder() {
    return new Builder().setSstFiltered(sstFiltered);
  }

  /**
   * Builder of SnapshotInfo.
   */
  public static class Builder {
    private boolean sstFiltered;
    private String snapshotTableKey;
    private boolean snapshotPurged;

    public Builder() {
      sstFiltered = false;
      snapshotPurged = false;
    }

    public Builder setSstFiltered(boolean sstFiltered) {
      this.sstFiltered = sstFiltered;
      return this;
    }

    public Builder setSnapshotTableKey(String snapshotTableKey) {
      this.snapshotTableKey = snapshotTableKey;
      return this;
    }

    public Builder setSnapshotPurged(boolean snapshotPurged) {
      this.snapshotPurged = snapshotPurged;
      return this;
    }

    public SnapshotProperties build() {
      return new SnapshotProperties(this);
    }
  }

  /**
   * Creates SnapshotProperties protobuf from SnapshotProperties.
   */
  public OzoneManagerProtocolProtos.SnapshotProperties getProtobuf() {
    return OzoneManagerProtocolProtos.SnapshotProperties.newBuilder()
        .setSstFiltered(sstFiltered).setSnapshotTableKey(snapshotTableKey)
        .setSnapshotPurged(snapshotPurged).build();
  }

  /**
   * Parses SnapshotProperties protobuf and creates SnapshotProperties.
   * @param snapshotProperties protobuf
   * @return instance of SnapshotInfo
   */
  public static SnapshotProperties getFromProtobuf(
      OzoneManagerProtocolProtos.SnapshotProperties snapshotProperties) {

    SnapshotProperties.Builder snapshotProps = SnapshotProperties.newBuilder();
    if (snapshotProperties.hasSstFiltered()) {
      snapshotProps.setSstFiltered(snapshotProperties.getSstFiltered());
    }
    if (snapshotProperties.hasSnapshotPurged()) {
      snapshotProps.setSnapshotPurged(snapshotProperties.getSnapshotPurged());
    }
    return snapshotProps.setSnapshotTableKey(snapshotProperties.getSnapshotTableKey()).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SnapshotProperties that = (SnapshotProperties) o;

    if (sstFiltered != that.sstFiltered) {
      return false;
    }
    return snapshotTableKey.equals(that.snapshotTableKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sstFiltered, snapshotTableKey);
  }

  @Override
  public String toString() {
    return "SnapshotProperties{" +
        "sstFiltered=" + sstFiltered +
        ", snapshotTableKey='" + snapshotTableKey + '\'' +
        '}';
  }
}
