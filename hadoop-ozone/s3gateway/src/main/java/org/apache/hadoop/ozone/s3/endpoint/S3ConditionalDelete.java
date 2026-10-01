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

package org.apache.hadoop.ozone.s3.endpoint;

import static org.apache.hadoop.ozone.s3.util.S3Utils.parseETag;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;

/**
 * Shared OM delete execution for S3 conditional deletes (single and multi).
 */
final class S3ConditionalDelete {

  enum Outcome {
    DELETED,
    /** Unconditional delete when the key is already absent (S3 idempotent delete). */
    NOT_FOUND_UNCONDITIONAL,
    PRECONDITION_FAILED,
    /** PREFIX layout: non-recursive delete of non-empty directory (S3 returns success). */
    DIRECTORY_NOT_EMPTY,
    FAILED
  }

  static final class Result {
    private final Outcome outcome;
    private final OMException omException;

    private Result(Outcome outcome, OMException omException) {
      this.outcome = outcome;
      this.omException = omException;
    }

    Outcome getOutcome() {
      return outcome;
    }

    OMException getOmException() {
      return omException;
    }
  }

  private S3ConditionalDelete() {
  }

  static Result deleteKey(ClientProtocol protocol, String volumeName,
      String bucketName, String keyName, String deletePreconditionEtag)
      throws IOException {
    boolean conditional = StringUtils.isNotBlank(deletePreconditionEtag);
    try {
      if (conditional) {
        protocol.deleteKey(volumeName, bucketName, keyName, false,
            parseETag(deletePreconditionEtag.trim()));
      } else {
        protocol.deleteKey(volumeName, bucketName, keyName, false);
      }
      return new Result(Outcome.DELETED, null);
    } catch (OMException ex) {
      return mapOmException(ex, conditional);
    }
  }

  private static Result mapOmException(OMException ex, boolean conditional) {
    ResultCodes code = ex.getResult();
    if (code == ResultCodes.KEY_NOT_FOUND) {
      if (conditional) {
        return new Result(Outcome.PRECONDITION_FAILED, ex);
      }
      return new Result(Outcome.NOT_FOUND_UNCONDITIONAL, ex);
    }
    if (code == ResultCodes.ETAG_MISMATCH || code == ResultCodes.ETAG_NOT_AVAILABLE) {
      return new Result(Outcome.PRECONDITION_FAILED, ex);
    }
    if (code == ResultCodes.DIRECTORY_NOT_EMPTY) {
      return new Result(Outcome.DIRECTORY_NOT_EMPTY, ex);
    }
    return new Result(Outcome.FAILED, ex);
  }
}
