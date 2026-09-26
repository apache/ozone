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

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ipc_.ProcessingDetails;
import org.apache.hadoop.ipc_.ProcessingDetails.Timing;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMLockDetailsProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/** Converts OM lock details between internal request processing and protocol responses. */
public final class OMLockDetailsUtil {

  private OMLockDetailsUtil() {
  }

  public static OMResponse addToResponse(OMResponse response, ProcessingDetails processingDetails) {
    long waitNanos = processingDetails.get(Timing.LOCKWAIT, TimeUnit.NANOSECONDS);
    long readNanos = processingDetails.get(Timing.LOCKSHARED, TimeUnit.NANOSECONDS);
    long writeNanos = processingDetails.get(Timing.LOCKEXCLUSIVE, TimeUnit.NANOSECONDS);
    if (waitNanos == 0 && readNanos == 0 && writeNanos == 0) {
      return response;
    }

    OMLockDetailsProto.Builder lockDetails = response.hasOmLockDetails()
        ? response.getOmLockDetails().toBuilder() : OMLockDetailsProto.newBuilder();
    lockDetails.setWaitLockNanos(lockDetails.getWaitLockNanos() + waitNanos);
    lockDetails.setReadLockNanos(lockDetails.getReadLockNanos() + readNanos);
    lockDetails.setWriteLockNanos(lockDetails.getWriteLockNanos() + writeNanos);
    return response.toBuilder().setOmLockDetails(lockDetails).build();
  }

  public static OMResponse addToResponse(OMResponse response, OMLockDetails lockDetails) {
    return lockDetails == null ? response
        : response.toBuilder().setOmLockDetails(lockDetails.toProtobufBuilder()).build();
  }

  public static void addToProcessingDetails(ProcessingDetails processingDetails, OMLockDetailsProto lockDetails) {
    processingDetails.add(Timing.LOCKWAIT, lockDetails.getWaitLockNanos(), TimeUnit.NANOSECONDS);
    processingDetails.add(Timing.LOCKSHARED, lockDetails.getReadLockNanos(), TimeUnit.NANOSECONDS);
    processingDetails.add(Timing.LOCKEXCLUSIVE, lockDetails.getWriteLockNanos(), TimeUnit.NANOSECONDS);
  }
}
