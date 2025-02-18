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

package org.apache.hadoop.hdds.conf;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link RatisConfUtils}.
 */
public class TestRatisConfUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TestRatisConfUtils.class);

  @Test
  void testGrpcSetMessageSizeMax() {
    final RaftProperties properties = new RaftProperties();

    final int logAppenderBufferByteLimit = 1000;

    // setMessageSizeMax without setBufferByteLimit
    Assertions.assertThrows(IllegalStateException.class,
        () -> RatisConfUtils.Grpc.setMessageSizeMax(properties, logAppenderBufferByteLimit));

    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(logAppenderBufferByteLimit));

    // setMessageSizeMax with a value smaller than logAppenderBufferByteLimit
    Assertions.assertThrows(IllegalStateException.class,
        () -> RatisConfUtils.Grpc.setMessageSizeMax(properties, logAppenderBufferByteLimit - 1));

    // setMessageSizeMax with the correct logAppenderBufferByteLimit
    RatisConfUtils.Grpc.setMessageSizeMax(properties, logAppenderBufferByteLimit);

    final SizeInBytes max = GrpcConfigKeys.messageSizeMax(properties, LOG::info);
    Assertions.assertEquals(SizeInBytes.ONE_MB.getSize(), max.getSize() - logAppenderBufferByteLimit);
  }
}
