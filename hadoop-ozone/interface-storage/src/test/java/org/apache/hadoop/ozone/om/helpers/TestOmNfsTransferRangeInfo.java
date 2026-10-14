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

package org.apache.hadoop.ozone.om.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsTransferRangeInfo;
import org.junit.jupiter.api.Test;

class TestOmNfsTransferRangeInfo {
  private static final ByteString SHA256 = ByteString.copyFrom(new byte[32]);

  @Test
  void testCodecRoundTrip() throws IOException {
    OmNfsTransferRangeInfo range = createRange(99);
    byte[] persisted = OmNfsTransferRangeInfo.getCodec().toPersistedFormat(range);
    assertEquals(range, OmNfsTransferRangeInfo.getCodec().fromPersistedFormat(persisted));
    assertEquals(99, OmNfsTransferRangeInfo.getCodec().fromPersistedFormat(persisted).getTransactionIndex());
  }

  @Test
  void testAcceptsUnsignedObjectIds() {
    OmNfsTransferRangeInfo range = new OmNfsTransferRangeInfo("export", Long.MIN_VALUE, Long.MIN_VALUE,
        Long.MIN_VALUE, 4, 5, 8, "transfer", "stage", SHA256);
    assertEquals(Long.MIN_VALUE, range.getObjectID());
  }

  @Test
  void testRejectsInvalidRange() {
    assertThrows(IllegalArgumentException.class,
        () -> new OmNfsTransferRangeInfo("export", 1, 2, 3, 4, -1, 8, "transfer", "stage", SHA256));
    assertThrows(IllegalArgumentException.class,
        () -> new OmNfsTransferRangeInfo("export", 1, 2, 3, 4, 0, 8, "transfer", "stage",
            ByteString.copyFrom(new byte[31])));
    assertThrows(ArithmeticException.class,
        () -> new OmNfsTransferRangeInfo("export", 1, 2, 3, 4, Long.MAX_VALUE, 1,
            "transfer", "stage", SHA256));
  }

  @Test
  void testRejectsIncompletePersistedRange() {
    PersistedNfsTransferRangeInfo partial = PersistedNfsTransferRangeInfo.newBuilder()
        .setExportUUID("export")
        .buildPartial();
    assertThrows(IllegalArgumentException.class,
        () -> OmNfsTransferRangeInfo.getFromProtobuf(partial));
  }

  private static OmNfsTransferRangeInfo createRange(long transactionIndex) {
    return new OmNfsTransferRangeInfo("export", 1, 2, 3, 4, 5, 8, "transfer", "stage", SHA256,
        transactionIndex);
  }
}
