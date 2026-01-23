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

package org.apache.ozone.erasurecode.rawcoder;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;

/**
 * A utility class that maintains encoding state during an encode call.
 */
@InterfaceAudience.Private
@SuppressWarnings("checkstyle:VisibilityModifier")
abstract class EncodingState {
  RawErasureEncoder encoder;
  int encodeLength;

  /**
   * Check and validate decoding parameters, throw exception accordingly.
   * @param inputs input buffers to check
   * @param outputs output buffers to check
   */
  <T> void checkParameters(T[] inputs, T[] outputs) {
    if (inputs.length != encoder.getNumDataUnits()) {
      throw new IllegalArgumentException("Invalid inputs length "
          + inputs.length + " !=" + encoder.getNumDataUnits());
    }
    if (outputs.length != encoder.getNumParityUnits()) {
      throw new IllegalArgumentException("Invalid outputs length "
          + outputs.length + " !=" + encoder.getNumParityUnits());
    }
  }
}
