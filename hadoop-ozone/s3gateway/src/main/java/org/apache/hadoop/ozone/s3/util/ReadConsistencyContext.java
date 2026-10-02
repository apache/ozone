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

package org.apache.hadoop.ozone.s3.util;

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.LOCAL_LEASE_LOG_LIMIT_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.READ_CONSISTENCY_FOLLOWER_LINEARIZABLE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.READ_CONSISTENCY_FOLLOWER_STALE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.READ_CONSISTENCY_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.READ_CONSISTENCY_LEADER_ONLY;

import java.util.Locale;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.helpers.ReadConsistency;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

/**
 * Read consistency values parsed from S3 request headers.
 */
public final class ReadConsistencyContext {
  private final ReadConsistency readConsistency;
  private final Long localLeaseLogLimit;

  private ReadConsistencyContext(ReadConsistency readConsistency,
      Long localLeaseLogLimit) {
    this.readConsistency = readConsistency;
    this.localLeaseLogLimit = localLeaseLogLimit;
  }

  public static ReadConsistencyContext fromHeaders(HttpHeaders headers) {
    return fromHeaders(headers.getHeaderString(READ_CONSISTENCY_HEADER),
        headers.getHeaderString(LOCAL_LEASE_LOG_LIMIT_HEADER));
  }

  public static ReadConsistencyContext fromHeaders(String readConsistencyHeader,
      String localLeaseLogLimitHeader) {
    ReadConsistency readConsistency = parseReadConsistency(readConsistencyHeader);
    Long localLeaseLogLimit = parseLocalLeaseLogLimit(localLeaseLogLimitHeader);
    if (readConsistency != ReadConsistency.LOCAL_LEASE
        && localLeaseLogLimit != null) {
      OS3Exception ex = newError(INVALID_ARGUMENT, READ_CONSISTENCY_HEADER);
      ex.setErrorMessage("Local lease context requires read consistency: "
          + READ_CONSISTENCY_FOLLOWER_STALE);
      throw ex;
    }
    return new ReadConsistencyContext(readConsistency, localLeaseLogLimit);
  }

  public ReadConsistency getReadConsistency() {
    return readConsistency;
  }

  public Long getLocalLeaseLogLimit() {
    return localLeaseLogLimit;
  }

  private static ReadConsistency parseReadConsistency(String header) {
    if (StringUtils.isBlank(header)) {
      return null;
    }
    switch (header.trim().toLowerCase(Locale.ROOT)) {
    case READ_CONSISTENCY_FOLLOWER_STALE:
      return ReadConsistency.LOCAL_LEASE;
    case READ_CONSISTENCY_LEADER_ONLY:
      return ReadConsistency.LINEARIZABLE_LEADER_ONLY;
    case READ_CONSISTENCY_FOLLOWER_LINEARIZABLE:
      return ReadConsistency.LINEARIZABLE_ALLOW_FOLLOWER;
    default:
      OS3Exception ex = newError(INVALID_ARGUMENT, READ_CONSISTENCY_HEADER);
      ex.setErrorMessage("Unsupported read consistency: " + header);
      throw ex;
    }
  }

  private static Long parseLocalLeaseLogLimit(String header) {
    if (StringUtils.isBlank(header)) {
      return null;
    }
    try {
      long value = Long.parseLong(header.trim());
      if (value < -1) {
        throw invalidLocalLeaseLogLimit(header);
      }
      return value;
    } catch (NumberFormatException e) {
      throw invalidLocalLeaseLogLimit(header);
    }
  }

  private static OS3Exception invalidLocalLeaseLogLimit(String header) {
    OS3Exception ex = newError(INVALID_ARGUMENT, LOCAL_LEASE_LOG_LIMIT_HEADER);
    ex.setErrorMessage("Invalid local lease context: " + header);
    return ex;
  }
}
