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
import java.time.Instant;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.db.Codec;

import com.google.common.base.Preconditions;

import static org.apache.hadoop.ozone.ClientVersions.CURRENT_VERSION;

/**
 * Codec to serialize / deserialize Pipeline.
 */
public class PipelineCodec implements Codec<Pipeline> {

  @Override
  public byte[] toPersistedFormat(Pipeline object) throws IOException {
    return object.getProtobufMessage(CURRENT_VERSION).toByteArray();
  }

  @Override
  public Pipeline fromPersistedFormat(byte[] rawData) throws IOException {
    HddsProtos.Pipeline.Builder pipelineBuilder = HddsProtos.Pipeline
        .newBuilder(HddsProtos.Pipeline.PARSER.parseFrom(rawData));
    Pipeline pipeline = Pipeline.getFromProtobuf(pipelineBuilder.build());
    // When SCM is restarted, set Creation time with current time.
    pipeline.setCreationTimestamp(Instant.now());
    Preconditions.checkNotNull(pipeline);
    return pipeline;
  }

  @Override
  public Pipeline copyObject(Pipeline object) {
    throw new UnsupportedOperationException();
  }
}
