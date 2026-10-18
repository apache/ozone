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

package org.apache.hadoop.ozone.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;

/** Utilities for tests using Ozone client. */
public final class OzoneClientTestUtils {

  /** Verify contents of a key.
   * @return key details for convenience (further checks) */
  public static OzoneKeyDetails assertKeyContent(
      OzoneBucket bucket,
      String keyName,
      String expected
  ) throws IOException {
    return assertKeyContent(bucket, keyName, expected.getBytes(UTF_8));
  }

  /** Verify contents of a key.
   * @return key details for convenience (further checks) */
  public static OzoneKeyDetails assertKeyContent(
      OzoneBucket bucket,
      String keyName,
      byte[] expected
  ) throws IOException {
    try (InputStream is = bucket.readKey(keyName)) {
      assertArrayEquals(expected, IOUtils.readFully(is, expected.length));
    }
    return bucket.getKey(keyName);
  }

  private OzoneClientTestUtils() {
    // no instances
  }
}
