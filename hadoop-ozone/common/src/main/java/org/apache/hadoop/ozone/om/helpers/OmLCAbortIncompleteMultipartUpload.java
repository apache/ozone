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

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AbortIncompleteMultipartUpload;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleAction;

/**
 * A class that encapsulates lifecycle rule AbortIncompleteMultipartUpload action.
 * This class extends OmLCAction and represents the AbortIncompleteMultipartUpload
 * action type in lifecycle configuration.
 */
public final class OmLCAbortIncompleteMultipartUpload implements OmLCAction {
  private final Integer daysAfterInitiation;
  private long daysInMilli;

  private OmLCAbortIncompleteMultipartUpload() {
    throw new UnsupportedOperationException("Default constructor is not supported. Use Builder.");
  }

  private OmLCAbortIncompleteMultipartUpload(Builder builder) {
    this.daysAfterInitiation = builder.daysAfterInitiation;
  }

  @Nullable
  public Integer getDaysAfterInitiation() {
    return daysAfterInitiation;
  }

  /**
   * Checks if a multipart upload is eligible for abort based on its creation time.
   *
   * @param creationTimestamp The creation time of the multipart upload in milliseconds since epoch
   * @return true if the upload should be aborted, false otherwise
   */
  public boolean shouldAbort(long creationTimestamp) {
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    ZonedDateTime dateTime = ZonedDateTime.ofInstant(
        Instant.ofEpochMilli(creationTimestamp + daysInMilli), ZoneOffset.UTC);
    return now.isAfter(dateTime);
  }

  @Override
  public ActionType getActionType() {
    return ActionType.ABORT_INCOMPLETE_MULTIPART_UPLOAD;
  }

  /**
   * Validates the AbortIncompleteMultipartUpload configuration.
   * - DaysAfterInitiation must be specified
   * - DaysAfterInitiation must be a positive number greater than zero
   *
   * @param creationTime The creation time of the lifecycle configuration in milliseconds since epoch
   * @throws OMException if the validation fails
   */
  @Override
  public void valid(long creationTime) throws OMException {
    if (daysAfterInitiation == null) {
      throw new OMException("Invalid lifecycle configuration: 'DaysAfterInitiation' " +
          "must be specified for AbortIncompleteMultipartUpload action.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (daysAfterInitiation <= 0) {
      throw new OMException("'DaysAfterInitiation' for AbortIncompleteMultipartUpload action " +
          "must be a positive integer greater than zero.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    daysInMilli = TimeUnit.DAYS.toMillis(daysAfterInitiation);
  }

  @Override
  public LifecycleAction getProtobuf() {
    AbortIncompleteMultipartUpload.Builder builder = AbortIncompleteMultipartUpload.newBuilder();

    if (daysAfterInitiation != null) {
      builder.setDaysAfterInitiation(daysAfterInitiation);
    }

    return LifecycleAction.newBuilder()
        .setAbortIncompleteMultipartUpload(builder).build();
  }

  public static OmLCAbortIncompleteMultipartUpload getFromProtobuf(
      AbortIncompleteMultipartUpload abortIncompleteMultipartUpload) {
    OmLCAbortIncompleteMultipartUpload.Builder builder = new Builder();

    if (abortIncompleteMultipartUpload.hasDaysAfterInitiation()) {
      builder.setDaysAfterInitiation(abortIncompleteMultipartUpload.getDaysAfterInitiation());
    }

    return builder.build();
  }

  @Override
  public String toString() {
    return "OmLCAbortIncompleteMultipartUpload{" +
        "daysAfterInitiation=" + daysAfterInitiation +
        '}';
  }

  /**
   * Builder of OmLCAbortIncompleteMultipartUpload.
   */
  public static class Builder {
    private Integer daysAfterInitiation = null;

    public Builder setDaysAfterInitiation(int days) {
      this.daysAfterInitiation = days;
      return this;
    }

    public OmLCAbortIncompleteMultipartUpload build() {
      return new OmLCAbortIncompleteMultipartUpload(this);
    }
  }
}
