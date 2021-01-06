/**
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
package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.CanUnbuffer;

import java.nio.ByteBuffer;

/**
 * Utility class to query streams for supported capabilities of Ozone.
 * Capability strings must be in lower case.
 */
final class OzoneStreamCapabilities {

  private OzoneStreamCapabilities() {
  }

  /**
   * Stream read(ByteBuffer) capability implemented by
   * {@link OzoneFSInputStream#read(ByteBuffer)}.
   *
   * TODO: If Hadoop dependency is upgraded, this string can be removed.
   */
  static final String READBYTEBUFFER = "in:readbytebuffer";

  /**
   * Stream unbuffer capability implemented by {@link CanUnbuffer#unbuffer()}.
   *
   * TODO: If Hadoop dependency is upgraded, this string can be removed.
   */
  static final String UNBUFFER = "in:unbuffer";
}
