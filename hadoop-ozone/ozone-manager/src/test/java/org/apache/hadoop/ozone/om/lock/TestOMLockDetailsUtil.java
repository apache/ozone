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

package org.apache.hadoop.ozone.om.lock;

import static org.apache.hadoop.ipc_.ProcessingDetails.Timing.LOCKEXCLUSIVE;
import static org.apache.hadoop.ipc_.ProcessingDetails.Timing.LOCKSHARED;
import static org.apache.hadoop.ipc_.ProcessingDetails.Timing.LOCKWAIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.RpcConstants;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMLockDetailsProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

class TestOMLockDetailsUtil {

  @Test
  void testAddProcessingDetailsToResponse() {
    Server.Call call = newCall();
    call.getProcessingDetails().add(LOCKWAIT, 11, TimeUnit.NANOSECONDS);
    call.getProcessingDetails().add(LOCKSHARED, 22, TimeUnit.NANOSECONDS);
    call.getProcessingDetails().add(LOCKEXCLUSIVE, 33, TimeUnit.NANOSECONDS);
    OMResponse response = newResponseBuilder()
        .setOmLockDetails(OMLockDetailsProto.newBuilder()
            .setIsLockAcquired(true)
            .setWaitLockNanos(1)
            .setReadLockNanos(2)
            .setWriteLockNanos(3))
        .build();

    OMResponse result = OMLockDetailsUtil.addToResponse(response, call.getProcessingDetails());

    assertTrue(result.getOmLockDetails().getIsLockAcquired());
    assertEquals(12, result.getOmLockDetails().getWaitLockNanos());
    assertEquals(24, result.getOmLockDetails().getReadLockNanos());
    assertEquals(36, result.getOmLockDetails().getWriteLockNanos());
  }

  @Test
  void testEmptyProcessingDetailsDoesNotChangeResponse() {
    OMResponse response = OMResponse.getDefaultInstance();

    assertSame(response, OMLockDetailsUtil.addToResponse(response, newCall().getProcessingDetails()));
  }

  @Test
  void testAddOmLockDetailsToResponse() {
    OMLockDetails details = new OMLockDetails();
    details.setLockAcquired(true);
    details.setWaitLockNanos(11);
    details.setReadLockNanos(22);
    details.setWriteLockNanos(33);
    OMResponse response = newResponseBuilder()
        .setOmLockDetails(OMLockDetailsProto.newBuilder().setWaitLockNanos(1))
        .build();

    OMResponse result = OMLockDetailsUtil.addToResponse(response, details);

    assertEquals(details.toProtobufBuilder().build(), result.getOmLockDetails());
  }

  @Test
  void testNullOmLockDetailsDoesNotChangeResponse() {
    OMResponse response = OMResponse.getDefaultInstance();

    assertSame(response, OMLockDetailsUtil.addToResponse(response, (OMLockDetails) null));
  }

  @Test
  void testAddToProcessingDetails() {
    Server.Call call = newCall();
    call.getProcessingDetails().add(LOCKWAIT, 1, TimeUnit.NANOSECONDS);
    call.getProcessingDetails().add(LOCKSHARED, 2, TimeUnit.NANOSECONDS);
    call.getProcessingDetails().add(LOCKEXCLUSIVE, 3, TimeUnit.NANOSECONDS);
    OMLockDetailsProto details = OMLockDetailsProto.newBuilder()
        .setWaitLockNanos(11)
        .setReadLockNanos(22)
        .setWriteLockNanos(33)
        .build();

    OMLockDetailsUtil.addToProcessingDetails(call.getProcessingDetails(), details);

    assertEquals(12, call.getProcessingDetails().get(LOCKWAIT, TimeUnit.NANOSECONDS));
    assertEquals(24, call.getProcessingDetails().get(LOCKSHARED, TimeUnit.NANOSECONDS));
    assertEquals(36, call.getProcessingDetails().get(LOCKEXCLUSIVE, TimeUnit.NANOSECONDS));
  }

  private static Server.Call newCall() {
    return new Server.Call(RpcConstants.INVALID_CALL_ID, RpcConstants.INVALID_RETRY_COUNT,
        null, null, RPC.RpcKind.RPC_PROTOCOL_BUFFER, RpcConstants.DUMMY_CLIENT_ID);
  }

  private static OMResponse.Builder newResponseBuilder() {
    return OMResponse.newBuilder()
        .setCmdType(Type.ServiceList)
        .setStatus(Status.OK);
  }
}
