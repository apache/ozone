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
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;

/**
 * Utilities for Ratis configurations.
 */
public class RatisConfUtils {
  /** For {@link GrpcConfigKeys}. */
  public static class Grpc {
    /** For setting {@link GrpcConfigKeys#setMessageSizeMax(RaftProperties, SizeInBytes)}. */
    public static void setMessageSizeMax(RaftProperties properties, int max) {
      Preconditions.assertTrue(max > 0, () -> "max = " + max + " <= 0");

      final long logAppenderBufferByteLimit = RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties).getSize();
      Preconditions.assertTrue(max >= logAppenderBufferByteLimit,
          () -> "max = " + max + " < logAppenderBufferByteLimit = " + logAppenderBufferByteLimit);

      // Need an 1MB gap; see RATIS-2135
      GrpcConfigKeys.setMessageSizeMax(properties, SizeInBytes.valueOf(max + SizeInBytes.ONE_MB.getSize()));
    }
  }
}
