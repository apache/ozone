/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is the interface for non-blocking flush/sync operations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface NonBlockingSyncable {

  /**
   * Flush out the data in client's user buffer.
   * Returns a future. After the future is completed, new readers will see the data.
   *
   * @throws IOException if any error occurs
   */
  void hflush() throws IOException;

  /**
   * Similar to posix fsync, flush out the data in client's user buffer
   * all the way to the disk device (but the disk may have it in its cache)
   * when the future returned is completed.
   *
   * @throws IOException if error occurs
   */
  void hsync() throws IOException;
}
