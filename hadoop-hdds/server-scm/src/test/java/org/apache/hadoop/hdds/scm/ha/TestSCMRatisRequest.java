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

package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.ratis.protocol.Message;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

import static org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType.PIPELINE;

/**
 * Test for SCMRatisRequest.
 */
public class TestSCMRatisRequest {

  @Test
  public void testEncodeAndDecodeSuccess() throws Exception {
    PipelineID pipelineID = PipelineID.randomId();
    ArrayList<HddsProtos.PipelineID> list = new ArrayList<>();
    list.add(pipelineID.getProtobuf());
    Object[] args = new Object[] {pipelineID.getProtobuf(), list, 1L};
    String operation = "test";
    SCMRatisRequest request = SCMRatisRequest.of(PIPELINE, operation, args);
    SCMRatisRequest decodeRequest = SCMRatisRequest.decode(request.encode());
    Assert.assertEquals(operation,
        decodeRequest.getOperation());
    Assert.assertEquals(args[0],
        decodeRequest.getArguments()[0]);
    Assert.assertEquals(((ArrayList)args[1]).get(0),
        ((ArrayList)decodeRequest.getArguments()[1]).get(0));
    Assert.assertEquals(args[2],
        decodeRequest.getArguments()[2]);
  }

  @Test(expected = InvalidProtocolBufferException.class)
  public void testEncodeWithNonProto() throws Exception{
    PipelineID pipelineID = PipelineID.randomId();
    // Non proto args
    Object[] args = new Object[] {pipelineID};
    SCMRatisRequest request = SCMRatisRequest.of(PIPELINE, "test", args);
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
}
