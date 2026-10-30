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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Test {@link OpenKeyDBKey}.
 */
public class TestOpenKeyDBKey {

  @Test
  public void testClientId() {
    // The key name may contain "/", only the part after the last "/" is the suffix
    OpenKeyDBKey openKey = new OpenKeyDBKey("/vol/bucket/dir/key/1000");
    assertFalse(openKey.isUpload());
    assertEquals(1000L, openKey.getClientId());
    assertNull(openKey.getUploadId());
    assertEquals("/vol/bucket/dir/key", openKey.getKeyTableDBKey());
  }

  @Test
  public void testUploadId() {
    String uploadId = "c1f1d5a2-3b4e-4c5d-8e9f-0a1b2c3d4e5f-123456789";
    // openKeyTable and openFileTable
    for (String keyTableDBKey : new String[] {"/vol/bucket/key", "/1/2/3/file"}) {
      OpenKeyDBKey openKey = new OpenKeyDBKey(keyTableDBKey + "/" + uploadId);
      assertTrue(openKey.isUpload());
      assertEquals(uploadId, openKey.getUploadId());
      assertEquals(keyTableDBKey, openKey.getKeyTableDBKey());
      assertThrows(IllegalStateException.class, openKey::getClientId);
    }
  }

  @Test
  public void testNotOpenKeyDBKey() {
    assertThrows(IllegalArgumentException.class, () -> new OpenKeyDBKey("1000"));
  }
}
