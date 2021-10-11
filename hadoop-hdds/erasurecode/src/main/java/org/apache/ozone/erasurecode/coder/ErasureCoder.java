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
package org.apache.ozone.erasurecode.coder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.ozone.erasurecode.ECBlockGroup;

/**
 * An erasure coder to perform encoding or decoding given a group. Generally it
 * involves calculating necessary internal steps according to codec logic. For
 * each step,it calculates necessary input blocks to read chunks from and output
 * parity blocks to write parity chunks into from the group. It also takes care
 * of appropriate raw coder to use for the step. And encapsulates all the
 * necessary info (input blocks, output blocks and raw coder) into a step
 * represented by {@link ErasureCodingStep}. ErasureCoder callers can use the
 * step to do the real work with retrieved input and output chunks.
 *
 * Note, currently only one coding step is supported. Will support complex cases
 * of multiple coding steps.
 *
 */
@InterfaceAudience.Private
public interface ErasureCoder {

  /**
   * The number of data input units for the coding. A unit can be a byte, chunk
   * or buffer or even a block.
   * @return count of data input units
   */
  int getNumDataUnits();

  /**
   * The number of parity output units for the coding. A unit can be a byte,
   * chunk, buffer or even a block.
   * @return count of parity output units
   */
  int getNumParityUnits();

  /**
   * The options of erasure coder. This option is passed to
   * raw erasure coder as it is.
   * @return erasure coder options
   */
  ECReplicationConfig getOptions();

  /**
   * Calculate the encoding or decoding steps given a block blockGroup.
   *
   * Note, currently only one coding step is supported. Will support complex
   * cases of multiple coding steps.
   *
   * @param blockGroup the erasure coding block group containing all necessary
   *                   information for codec calculation
   */
  ErasureCodingStep calculateCoding(ECBlockGroup blockGroup);

  /**
   * Tell if direct or off-heap buffer is preferred or not. It's for callers to
   * decide how to allocate coding chunk buffers, either on heap or off heap.
   * It will return false by default.
   * @return true if direct buffer is preferred for performance consideration,
   * otherwise false.
   */
  boolean preferDirectBuffer();

  /**
   * Release the resources if any. Good chance to invoke
   * RawErasureCoder#release.
   */
  void release();
}
