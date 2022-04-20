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
package org.apache.ozone.erasurecode.rawcoder;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ECReplicationConfig;

/**
 * A raw coder factory for raw Reed-Solomon coder in native using Intel ISA-L.
 */
@InterfaceAudience.Private
public class NativeRSRawErasureCoderFactory
    implements RawErasureCoderFactory {

  public static final String CODER_NAME = "rs_native";

  @Override
  public RawErasureEncoder createEncoder(
      ECReplicationConfig ecReplicationConfig) {
    return new NativeRSRawEncoder(ecReplicationConfig);
  }

  @Override
  public RawErasureDecoder createDecoder(
      ECReplicationConfig ecReplicationConfig) {
    return new NativeRSRawDecoder(ecReplicationConfig);
  }

  @Override
  public String getCoderName() {
    return CODER_NAME;
  }

  @Override
  public String getCodecName() {
    return ECReplicationConfig.EcCodec.RS.name().toLowerCase();
  }
}