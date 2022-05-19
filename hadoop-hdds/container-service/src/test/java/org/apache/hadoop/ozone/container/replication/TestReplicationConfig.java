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
package org.apache.hadoop.ozone.container.replication;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig;
import org.junit.Test;

import static org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig.REPLICATION_MAX_STREAMS_DEFAULT;
import static org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig.REPLICATION_STREAMS_LIMIT_KEY;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ReplicationConfig}.
 */
public class TestReplicationConfig {

  @Test
  public void acceptsValidValues() {
    // GIVEN
    int validReplicationLimit = 123;
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(REPLICATION_STREAMS_LIMIT_KEY, validReplicationLimit);

    // WHEN
    ReplicationConfig subject = conf.getObject(ReplicationConfig.class);

    // THEN
    assertEquals(validReplicationLimit, subject.getReplicationMaxStreams());
  }

  @Test
  public void overridesInvalidValues() {
    // GIVEN
    int invalidReplicationLimit = -5;
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(REPLICATION_STREAMS_LIMIT_KEY, invalidReplicationLimit);

    // WHEN
    ReplicationConfig subject = conf.getObject(ReplicationConfig.class);

    // THEN
    assertEquals(REPLICATION_MAX_STREAMS_DEFAULT,
        subject.getReplicationMaxStreams());
  }

  @Test
  public void isCreatedWitDefaultValues() {
    // GIVEN
    OzoneConfiguration conf = new OzoneConfiguration();

    // WHEN
    ReplicationConfig subject = conf.getObject(ReplicationConfig.class);

    // THEN
    assertEquals(REPLICATION_MAX_STREAMS_DEFAULT,
        subject.getReplicationMaxStreams());
  }

}
