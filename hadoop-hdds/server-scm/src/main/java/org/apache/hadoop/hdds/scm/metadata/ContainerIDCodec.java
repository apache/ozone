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
package org.apache.hadoop.hdds.scm.metadata;

import java.io.IOException;

import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.LongCodec;

/**
 * Codec to serialize / deserialize ContainerID.
 */
public class ContainerIDCodec implements Codec<ContainerID> {

  private Codec<Long> longCodec = new LongCodec();

  @Override
  public byte[] toPersistedFormat(ContainerID container) throws IOException {
    return longCodec.toPersistedFormat(container.getId());
  }

  @Override
  public ContainerID fromPersistedFormat(byte[] rawData) throws IOException {
    return ContainerID.valueOf(longCodec.fromPersistedFormat(rawData));
  }

  @Override
  public ContainerID copyObject(ContainerID object) {
    return ContainerID.valueOf(object.getId());
  }
}
