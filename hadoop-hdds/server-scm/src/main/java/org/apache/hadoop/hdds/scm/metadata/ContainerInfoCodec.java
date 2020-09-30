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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.utils.db.Codec;

/**
 * Codec to serialize / deserialize ContainerInfo.
 */
public class ContainerInfoCodec implements Codec<ContainerInfo> {

  @Override
  public byte[] toPersistedFormat(ContainerInfo container) throws IOException {
    return container.getProtobuf().toByteArray();
  }

  @Override
  public ContainerInfo fromPersistedFormat(byte[] rawData) throws IOException {
    return ContainerInfo.fromProtobuf(
        ContainerInfoProto.PARSER.parseFrom(rawData));
  }

  @Override
  public ContainerInfo copyObject(ContainerInfo object) {
    throw new UnsupportedOperationException();
  }
}
