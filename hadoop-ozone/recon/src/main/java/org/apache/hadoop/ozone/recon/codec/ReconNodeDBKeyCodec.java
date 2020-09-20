/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.recon.codec;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.Codec;

/**
 * Codec for UUID.
 */
public class ReconNodeDBKeyCodec implements Codec<UUID> {
  @Override
  public byte[] toPersistedFormat(UUID object) throws IOException {
    return StringUtils.string2Bytes(object.toString());
  }

  @Override
  public UUID fromPersistedFormat(byte[] rawData) throws IOException {
    return UUID.fromString(StringUtils.bytes2String(rawData));
  }

  @Override
  public UUID copyObject(UUID object) {
    return null;
  }
}
