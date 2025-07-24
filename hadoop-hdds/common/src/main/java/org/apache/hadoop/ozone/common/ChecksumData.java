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

package org.apache.hadoop.ozone.common;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import net.jcip.annotations.Immutable;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.MemoizedSupplier;

/**
 * Java class that represents Checksum ProtoBuf class. This helper class allows
 * us to convert to and from protobuf to normal java.
 * <p>
 * This class is immutable.
 */
@Immutable
public final class ChecksumData {

  private final ChecksumType type;
  // Checksum will be computed for every bytesPerChecksum number of bytes and
  // stored sequentially in checksumList
  private final int bytesPerChecksum;
  private final List<ByteString> checksums;
  private final Supplier<ContainerProtos.ChecksumData> protoSupplier;

  public ChecksumData(ChecksumType checksumType, int bytesPerChecksum) {
    this(checksumType, bytesPerChecksum, Collections.emptyList());
  }

  public ChecksumData(ChecksumType type, int bytesPerChecksum, List<ByteString> checksums) {
    this.type = Objects.requireNonNull(type, "type == null");
    this.bytesPerChecksum = bytesPerChecksum;
    this.checksums = Collections.unmodifiableList(checksums);

    this.protoSupplier = MemoizedSupplier.valueOf(() -> ContainerProtos.ChecksumData.newBuilder()
        .setType(getChecksumType())
        .setBytesPerChecksum(getBytesPerChecksum())
        .addAllChecksums(getChecksums()).build());
  }

  /**
   * Getter method for checksumType.
   */
  public ChecksumType getChecksumType() {
    return this.type;
  }

  /**
   * Getter method for bytesPerChecksum.
   */
  public int getBytesPerChecksum() {
    return this.bytesPerChecksum;
  }

  /**
   * Getter method for checksums.
   */
  public List<ByteString> getChecksums() {
    return this.checksums;
  }

  /**
   * Construct the Checksum ProtoBuf message.
   * @return Checksum ProtoBuf message
   */
  public ContainerProtos.ChecksumData getProtoBufMessage() {
    return protoSupplier.get();
  }

  /**
   * Constructs Checksum class object from the Checksum ProtoBuf message.
   * @param checksumDataProto Checksum ProtoBuf message
   * @return ChecksumData object representing the proto
   */
  public static ChecksumData getFromProtoBuf(
      ContainerProtos.ChecksumData checksumDataProto) {
    Objects.requireNonNull(checksumDataProto, "checksumDataProto == null");

    return new ChecksumData(
        checksumDataProto.getType(),
        checksumDataProto.getBytesPerChecksum(),
        checksumDataProto.getChecksumsList());
  }

  /**
   * Verify that this ChecksumData from thisStartIndex matches with the provided ChecksumData.
   *
   * @param thisStartIndex the index of the first checksum in this object to be verified
   * @param that the ChecksumData to match with
   * @throws OzoneChecksumException if checksums mismatched.
   */
  public void verifyChecksumDataMatches(int thisStartIndex, ChecksumData that) throws OzoneChecksumException {
    final int thisChecksumsCount = this.checksums.size();
    final int thatChecksumsCount = that.checksums.size();
    if (thatChecksumsCount > thisChecksumsCount - thisStartIndex) {
      throw new OzoneChecksumException("Checksum count mismatched: thatChecksumsCount=" + thatChecksumsCount
          + " > thisChecksumsCount (=" + thisChecksumsCount + " ) - thisStartIndex (=" + thisStartIndex + ")");
    }

    // Verify that checksum matches at each index
    for (int i = 0; i < thatChecksumsCount; i++) {
      final int j = i + thisStartIndex;
      if (!this.checksums.get(j).equals(that.checksums.get(i))) {
        // checksum mismatch. throw exception.
        throw new OzoneChecksumException("Checksum mismatched: this.checksums(" + j + ") != that.checksums(" + i
            + "), thisStartIndex=" + thisStartIndex
            + ", this=" + this
            + ", that=" + that);
      }
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ChecksumData)) {
      return false;
    }

    final ChecksumData that = (ChecksumData) obj;
    return Objects.equals(this.getChecksumType(), that.getChecksumType())
        && Objects.equals(this.getBytesPerChecksum(), that.getBytesPerChecksum())
        && Objects.equals(this.getChecksums(), that.getChecksums());
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hc = new HashCodeBuilder();
    hc.append(type);
    hc.append(bytesPerChecksum);
    hc.append(checksums.toArray());
    return hc.toHashCode();
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder("ChecksumData{")
        .append(type)
        .append(", bytesPerChecksum=").append(bytesPerChecksum)
        .append(", checksums=[");
    if (!checksums.isEmpty()) {
      for (ByteString checksum : checksums) {
        b.append(StringUtils.bytes2Hex(checksum.asReadOnlyByteBuffer())).append(", ");
      }
      b.setLength(b.length() - 2);
    }
    return b.append("]}").toString();
  }
}
