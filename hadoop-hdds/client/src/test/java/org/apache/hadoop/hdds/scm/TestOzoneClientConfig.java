/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestOzoneClientConfig {

  @Test
  void missingSizeSuffix() {
    final int bytes = 1024;

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt("ozone.client.bytes.per.checksum", bytes);

    OzoneClientConfig subject = conf.getObject(OzoneClientConfig.class);

    assertEquals(OZONE_CLIENT_BYTES_PER_CHECKSUM_MIN_SIZE, subject.getBytesPerChecksum());
  }
}
