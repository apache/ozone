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
package org.apache.ozone.erasurecode.coder;


import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.ozone.erasurecode.ECBlock;
import org.apache.ozone.erasurecode.ECBlockGroup;
import org.apache.ozone.erasurecode.rawcoder.DummyRawDecoder;
import org.apache.ozone.erasurecode.rawcoder.RawErasureDecoder;

/**n
 * Dummy erasure decoder does no real computation. Instead, it just returns
 * zero bytes. This decoder can be used to isolate the performance issue to
 * HDFS side logic instead of codec, and is intended for test only.
 */
public class DummyErasureDecoder extends ErasureDecoder {
  public DummyErasureDecoder(ECReplicationConfig ecReplicationConfig) {
    super(ecReplicationConfig);
  }

  @Override
  protected ErasureCodingStep prepareDecodingStep(ECBlockGroup blockGroup) {
    RawErasureDecoder rawDecoder = new DummyRawDecoder(getOptions());

    ECBlock[] inputBlocks = getInputBlocks(blockGroup);

    return new ErasureDecodingStep(inputBlocks,
        getErasedIndexes(inputBlocks),
        getOutputBlocks(blockGroup), rawDecoder);
  }
}
