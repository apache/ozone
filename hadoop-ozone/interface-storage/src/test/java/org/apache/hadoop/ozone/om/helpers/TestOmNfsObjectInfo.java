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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsObjectInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestOmNfsObjectInfo {
  @ParameterizedTest
  @EnumSource(PersistedNfsObjectInfo.Kind.class)
  void testCodecPreservesPointer(PersistedNfsObjectInfo.Kind kind) throws IOException {
    for (long parent : new long[] {0, Long.MIN_VALUE, Long.MAX_VALUE, -1}) {
      OmNfsObjectInfo original = new OmNfsObjectInfo(kind, parent, "café 文件");
      OmNfsObjectInfo restored = OmNfsObjectInfo.getCodec().fromPersistedFormat(
          OmNfsObjectInfo.getCodec().toPersistedFormat(original));
      assertEquals(original, restored);
      assertEquals(original.hashCode(), restored.hashCode());
      assertEquals(parent, restored.getParentObjectID());
      assertEquals(kind, restored.getKind());
      assertEquals("café 文件", restored.getName());
      assertSame(original, OmNfsObjectInfo.getCodec().copyObject(original));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", ".", "..", "/file", "parent/file", "file/"})
  void testRejectsNonLeafNames(String name) {
    assertThrows(IllegalArgumentException.class,
        () -> new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, 1, name));
  }

  @Test
  void testNullFields() {
    assertThrows(NullPointerException.class, () -> new OmNfsObjectInfo(null, 1, "file"));
    assertThrows(NullPointerException.class, () -> new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, 1, null));
  }

  @Test
  void testIncompletePersistedRecordIsRejected() {
    assertThrows(IOException.class, () -> OmNfsObjectInfo.getCodec().fromPersistedFormat(new byte[0]));
  }

  @Test
  void testPersistedFormatContract() throws IOException {
    byte[] expected = {8, 2, 16, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, 26, 1, 120};
    OmNfsObjectInfo file = new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, -1, "x");
    assertArrayEquals(expected, OmNfsObjectInfo.getCodec().toPersistedFormat(file));
    assertEquals(file, OmNfsObjectInfo.getCodec().fromPersistedFormat(expected));
  }

  @Test
  void testUnknownPersistedKindIsRejected() {
    byte[] unknownKind = {8, 3, 16, 1, 26, 1, 120};
    assertThrows(IOException.class, () -> OmNfsObjectInfo.getCodec().fromPersistedFormat(unknownKind));
  }

  @Test
  void testPartialProtobufIsRejected() {
    PersistedNfsObjectInfo partial = PersistedNfsObjectInfo.newBuilder().setParentObjectID(1).setName("file")
        .buildPartial();
    assertThrows(IllegalArgumentException.class, () -> OmNfsObjectInfo.getFromProtobuf(partial));
  }

  @Test
  void testPointerChangesAreDistinct() {
    OmNfsObjectInfo file = new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, 1, "file");
    assertNotEquals(file, new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.DIRECTORY, 1, "file"));
    assertNotEquals(file, new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, 2, "file"));
    assertNotEquals(file, new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, 1, "renamed"));
  }
}
