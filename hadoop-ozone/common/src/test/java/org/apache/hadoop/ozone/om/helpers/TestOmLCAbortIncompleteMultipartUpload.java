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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.helpers.OMLCUtils.assertOMException;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AbortIncompleteMultipartUpload;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleAction;
import org.junit.jupiter.api.Test;

/**
 * Test OmLCAbortIncompleteMultipartUpload.
 */
class TestOmLCAbortIncompleteMultipartUpload {

  @Test
  public void testCreateValidAbortIncompleteMultipartUpload() {
    long currentTime = System.currentTimeMillis();

    OmLCAbortIncompleteMultipartUpload.Builder abort1 =
        new OmLCAbortIncompleteMultipartUpload.Builder()
            .setDaysAfterInitiation(1);
    assertDoesNotThrow(() -> abort1.build().valid(currentTime));

    OmLCAbortIncompleteMultipartUpload.Builder abort2 =
        new OmLCAbortIncompleteMultipartUpload.Builder()
            .setDaysAfterInitiation(7);
    assertDoesNotThrow(() -> abort2.build().valid(currentTime));

    OmLCAbortIncompleteMultipartUpload.Builder abort3 =
        new OmLCAbortIncompleteMultipartUpload.Builder()
            .setDaysAfterInitiation(365);
    assertDoesNotThrow(() -> abort3.build().valid(currentTime));
  }

  @Test
  public void testCreateInvalidAbortIncompleteMultipartUpload() {
    long currentTime = System.currentTimeMillis();

    // Null days should fail
    OmLCAbortIncompleteMultipartUpload.Builder abort1 =
        new OmLCAbortIncompleteMultipartUpload.Builder();
    assertOMException(() -> abort1.build().valid(currentTime), INVALID_REQUEST,
        "must be specified for AbortIncompleteMultipartUpload action");

    // Zero days should fail
    OmLCAbortIncompleteMultipartUpload.Builder abort2 =
        new OmLCAbortIncompleteMultipartUpload.Builder()
            .setDaysAfterInitiation(0);
    assertOMException(() -> abort2.build().valid(currentTime), INVALID_REQUEST,
        "must be a positive integer greater than zero");

    // Negative days should fail
    OmLCAbortIncompleteMultipartUpload.Builder abort3 =
        new OmLCAbortIncompleteMultipartUpload.Builder()
            .setDaysAfterInitiation(-1);
    assertOMException(() -> abort3.build().valid(currentTime), INVALID_REQUEST,
        "must be a positive integer greater than zero");

    OmLCAbortIncompleteMultipartUpload.Builder abort4 =
        new OmLCAbortIncompleteMultipartUpload.Builder()
            .setDaysAfterInitiation(-100);
    assertOMException(() -> abort4.build().valid(currentTime), INVALID_REQUEST,
        "must be a positive integer greater than zero");
  }

  @Test
  public void testShouldAbort() throws OMException {
    long currentTime = System.currentTimeMillis();

    // Upload created 10 days ago
    long uploadCreationTime = currentTime - TimeUnit.DAYS.toMillis(10);

    // Rule: abort after 7 days - should abort
    OmLCAbortIncompleteMultipartUpload abort7Days =
        new OmLCAbortIncompleteMultipartUpload.Builder()
            .setDaysAfterInitiation(7)
            .build();
    abort7Days.valid(currentTime);
    assertTrue(abort7Days.shouldAbort(uploadCreationTime),
        "Upload created 10 days ago should be aborted with 7-day threshold");

    // Rule: abort after 15 days - should NOT abort
    OmLCAbortIncompleteMultipartUpload abort15Days =
        new OmLCAbortIncompleteMultipartUpload.Builder()
            .setDaysAfterInitiation(15)
            .build();
    abort15Days.valid(currentTime);
    assertFalse(abort15Days.shouldAbort(uploadCreationTime),
        "Upload created 10 days ago should NOT be aborted with 15-day threshold");

    // Upload created 1 hour ago - should NOT abort
    long recentUploadTime = currentTime - TimeUnit.HOURS.toMillis(1);
    OmLCAbortIncompleteMultipartUpload abort1Day =
        new OmLCAbortIncompleteMultipartUpload.Builder()
            .setDaysAfterInitiation(1)
            .build();
    abort1Day.valid(currentTime);
    assertFalse(abort1Day.shouldAbort(recentUploadTime),
        "Upload created 1 hour ago should NOT be aborted with 1-day threshold");

    // Upload created 1 day + 1 second ago - should abort
    long moreThanOneDay = currentTime - TimeUnit.DAYS.toMillis(1) - TimeUnit.SECONDS.toMillis(1);
    assertTrue(abort1Day.shouldAbort(moreThanOneDay),
        "Upload created more than 1 day ago should be aborted");
  }

  @Test
  public void testProtobufConversion() throws OMException {
    long currentTime = System.currentTimeMillis();

    // Create with 7 days
    OmLCAbortIncompleteMultipartUpload original =
        new OmLCAbortIncompleteMultipartUpload.Builder()
            .setDaysAfterInitiation(7)
            .build();

    // Convert to protobuf
    LifecycleAction proto = original.getProtobuf();
    assertTrue(proto.hasAbortIncompleteMultipartUpload(),
        "Protobuf should have AbortIncompleteMultipartUpload");

    AbortIncompleteMultipartUpload abortProto = proto.getAbortIncompleteMultipartUpload();
    assertEquals(7, abortProto.getDaysAfterInitiation(),
        "Days should be preserved in protobuf");

    // Convert back from protobuf
    OmLCAbortIncompleteMultipartUpload fromProto =
        OmLCAbortIncompleteMultipartUpload.getFromProtobuf(abortProto);
    assertEquals(7, fromProto.getDaysAfterInitiation(),
        "Days should be preserved after protobuf round-trip");

    // Validate the converted object
    assertDoesNotThrow(() -> fromProto.valid(currentTime),
        "Object from protobuf should be valid");
  }

  @Test
  public void testActionType() {
    OmLCAbortIncompleteMultipartUpload abort =
        new OmLCAbortIncompleteMultipartUpload.Builder()
            .setDaysAfterInitiation(7)
            .build();

    assertEquals(OmLCAction.ActionType.ABORT_INCOMPLETE_MULTIPART_UPLOAD,
        abort.getActionType(),
        "Action type should be ABORT_INCOMPLETE_MULTIPART_UPLOAD");
  }
}
