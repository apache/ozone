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
package org.apache.hadoop.ozone.container.common.statemachine;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.Test;

import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.CONTAINER_DELETE_THREADS_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.CONTAINER_DELETE_THREADS_MAX_KEY;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.REPLICATION_MAX_STREAMS_DEFAULT;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.REPLICATION_STREAMS_LIMIT_KEY;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link DatanodeConfiguration}.
 */
public class TestDatanodeConfiguration {

  @Test
  public void acceptsValidValues() {
    // GIVEN
    int validReplicationLimit = 123;
    int validDeleteThreads = 42;
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(REPLICATION_STREAMS_LIMIT_KEY, validReplicationLimit);
    conf.setInt(CONTAINER_DELETE_THREADS_MAX_KEY, validDeleteThreads);

    // WHEN
    DatanodeConfiguration subject = conf.getObject(DatanodeConfiguration.class);

    // THEN
    assertEquals(validReplicationLimit, subject.getReplicationMaxStreams());
    assertEquals(validDeleteThreads, subject.getContainerDeleteThreads());
  }

  @Test
  public void overridesInvalidValues() {
    // GIVEN
    int invalidReplicationLimit = -5;
    int invalidDeleteThreads = 0;
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(REPLICATION_STREAMS_LIMIT_KEY, invalidReplicationLimit);
    conf.setInt(CONTAINER_DELETE_THREADS_MAX_KEY, invalidDeleteThreads);

    // WHEN
    DatanodeConfiguration subject = conf.getObject(DatanodeConfiguration.class);

    // THEN
    assertEquals(REPLICATION_MAX_STREAMS_DEFAULT,
        subject.getReplicationMaxStreams());
    assertEquals(CONTAINER_DELETE_THREADS_DEFAULT,
        subject.getContainerDeleteThreads());
  }

  @Test
  public void isCreatedWitDefaultValues() {
    // GIVEN
    OzoneConfiguration conf = new OzoneConfiguration();

    // WHEN
    DatanodeConfiguration subject = conf.getObject(DatanodeConfiguration.class);

    // THEN
    assertEquals(REPLICATION_MAX_STREAMS_DEFAULT,
        subject.getReplicationMaxStreams());
    assertEquals(CONTAINER_DELETE_THREADS_DEFAULT,
        subject.getContainerDeleteThreads());
  }

}
