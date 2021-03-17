/*
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

package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.ratis.protocol.Message;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType.PIPELINE;

/**
 * Test for SCMRatisRequest.
 */
public class TestSCMRatisRequest {

  @Test
  public void testEncodeAndDecodeSuccess() throws Exception {
    PipelineID pipelineID = PipelineID.randomId();
    Object[] args = new Object[] {pipelineID.getProtobuf()};
    String operation = "test";
    SCMRatisRequest request = SCMRatisRequest.of(PIPELINE, operation,
        new Class[]{pipelineID.getProtobuf().getClass()}, args);
    Assert.assertEquals(operation,
        SCMRatisRequest.decode(request.encode()).getOperation());
    Assert.assertEquals(args[0],
        SCMRatisRequest.decode(request.encode()).getArguments()[0]);
  }

  @Test(expected = InvalidProtocolBufferException.class)
  public void testEncodeWithNonProto() throws Exception{
    PipelineID pipelineID = PipelineID.randomId();
    // Non proto args
    Object[] args = new Object[] {pipelineID};
    SCMRatisRequest request = SCMRatisRequest.of(PIPELINE, "test",
        new Class[]{pipelineID.getClass()}, args);
    // Should throw exception there.
    request.encode();
  }

  @Test(expected = InvalidProtocolBufferException.class)
  public void testDecodeWithNonProto() throws Exception {
    // Non proto message
    Message message = Message.valueOf("randomMessage");
    // Should throw exception there.
    SCMRatisRequest.decode(message);
  }

  @Test
  public void testEncodeAndDecodeWithList() throws Exception {
    List<HddsProtos.PipelineID> pids = new ArrayList<>();
    pids.add(PipelineID.randomId().getProtobuf());
    pids.add(PipelineID.randomId().getProtobuf());
    pids.add(PipelineID.randomId().getProtobuf());
    Object[] args = new Object[] {pids};
    String operation = "test";
    SCMRatisRequest request = SCMRatisRequest.of(PIPELINE, operation,
        new Class[]{pids.getClass()}, args);
    Assert.assertEquals(operation,
        SCMRatisRequest.decode(request.encode()).getOperation());
    Assert.assertEquals(args[0],
        SCMRatisRequest.decode(request.encode()).getArguments()[0]);
  }

  @Test
  public void testEncodeAndDecodeOfLong() throws Exception {
    final Long value = 10L;
    String operation = "test";
    SCMRatisRequest request = SCMRatisRequest.of(PIPELINE, operation,
        new Class[]{value.getClass()}, value);
    Assert.assertEquals(operation,
        SCMRatisRequest.decode(request.encode()).getOperation());
    Assert.assertEquals(value,
        SCMRatisRequest.decode(request.encode()).getArguments()[0]);
  }
}
