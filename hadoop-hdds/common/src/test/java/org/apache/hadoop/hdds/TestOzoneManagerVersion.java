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

package org.apache.hadoop.hdds;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.junit.jupiter.api.Test;

/**
 * Invariants for {@link OzoneManagerVersion}.
 */
public class TestOzoneManagerVersion extends AbstractComponentVersionTest {

  @Override
  protected ComponentVersion[] getValues() {
    return OzoneManagerVersion.values();
  }

  @Override
  protected ComponentVersion getDefaultVersion() {
    return OzoneManagerVersion.DEFAULT_VERSION;
  }

  @Override
  protected ComponentVersion getFutureVersion() {
    return OzoneManagerVersion.FUTURE_VERSION;
  }

  @Override
  protected ComponentVersion deserialize(int value) {
    return OzoneManagerVersion.deserialize(value);
  }

  @Test
  public void testKnownVersionNumbersAreContiguousExceptForZDU() {
    OzoneManagerVersion[] values = OzoneManagerVersion.values();
    int knownVersionCount = values.length - 1;
    for (int i = 0; i < knownVersionCount - 1; i++) {
      OzoneManagerVersion current = values[i];
      OzoneManagerVersion next = values[i + 1];
      if (next == OzoneManagerVersion.ZDU) {
        assertEquals(100, next.serialize());
      } else {
        assertEquals(current.serialize() + 1, next.serialize());
      }
    }
  }
}
