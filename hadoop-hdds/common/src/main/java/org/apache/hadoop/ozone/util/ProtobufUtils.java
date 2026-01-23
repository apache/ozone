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

package org.apache.hadoop.ozone.util;

import com.google.protobuf.CodedOutputStream;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Contains utilities to ease common protobuf to java object conversions.
 */
public final class ProtobufUtils {
  private ProtobufUtils() {
  }

  public static HddsProtos.UUID toProtobuf(UUID uuid) {
    return HddsProtos.UUID.newBuilder()
        .setMostSigBits(uuid.getMostSignificantBits())
        .setLeastSigBits(uuid.getLeastSignificantBits())
        .build();
  }

  public static UUID fromProtobuf(HddsProtos.UUID proto) {
    return new UUID(proto.getMostSigBits(), proto.getLeastSigBits());
  }

  /**
   * Computes the serialized size of a string in a repeated string field.
   * Wraps protobuf's computeStringSizeNoTag for safer use.
   */
  public static int computeRepeatedStringSize(String value) {
    return CodedOutputStream.computeStringSizeNoTag(value);
  }

  public static int computeLongSizeWithTag(int fieldNumber, long value) {
    return CodedOutputStream.computeInt64Size(fieldNumber, value);
  }
}
