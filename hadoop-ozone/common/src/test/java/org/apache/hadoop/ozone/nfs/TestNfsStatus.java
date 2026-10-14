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

package org.apache.hadoop.ozone.nfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.nfs.NfsStatus.LookupContext;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests the backend error contract, independently of Ganesha session state. */
class TestNfsStatus {

  @Test
  void translatesAuthorizationIoFailuresToAccess() {
    assertEquals(NfsStatus.ACCESS, NfsStatus.fromIOException(new AuthorizationException("denied")));
    assertEquals(NfsStatus.ACCESS, NfsStatus.fromIOException(new RemoteException(
        AuthorizationException.class.getName(), "denied")));
    assertEquals(NfsStatus.IO, NfsStatus.fromIOException(new java.io.IOException("backend failure")));
  }

  @ParameterizedTest
  @EnumSource(value = ResultCodes.class, names = {
      "VOLUME_NOT_FOUND", "BUCKET_NOT_FOUND", "KEY_NOT_FOUND", "FILE_NOT_FOUND", "DIRECTORY_NOT_FOUND"})
  void missingNameAndStaleHandleAreDifferent(ResultCodes result) {
    OMException failure = new OMException(result);
    assertEquals(NfsStatus.NOENT, NfsStatus.fromOmException(failure, LookupContext.NAME));
    assertEquals(NfsStatus.STALE, NfsStatus.fromOmException(failure, LookupContext.HANDLE));
  }

  @ParameterizedTest
  @CsvSource({
      "ACCESS_DENIED, ACCESS", "PERMISSION_DENIED, ACCESS", "UNAUTHORIZED, ACCESS", "USER_MISMATCH, ACCESS",
      "INVALID_AUTH_METHOD, ACCESS", "INVALID_TOKEN, ACCESS", "TOKEN_EXPIRED, ACCESS",
      "KEY_ALREADY_EXISTS, EXIST", "FILE_ALREADY_EXISTS, EXIST", "DIRECTORY_ALREADY_EXISTS, EXIST",
      "NOT_A_FILE, ISDIR", "INVALID_KEY_NAME, INVAL", "INVALID_PATH, INVAL", "INVALID_REQUEST, INVAL",
      "DIRECTORY_NOT_EMPTY, NOTEMPTY", "BUCKET_NOT_EMPTY, NOTEMPTY", "QUOTA_EXCEEDED, DQUOT",
      "NOT_SUPPORTED_OPERATION, NOTSUPP", "NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION, NOTSUPP",
      "FEATURE_NOT_ENABLED, NOTSUPP", "OM_NOT_INITIALIZED, DELAY", "SCM_IN_SAFE_MODE, DELAY",
      "KEY_UNDER_LEASE_RECOVERY, DELAY", "KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD, DELAY",
      "NOT_SUPPORTED_OPERATION_WHEN_PREPARED, DELAY", "RENAME_OPEN_FILE, FILE_OPEN",
      "NFS_LEASE_CONFLICT, FILE_OPEN",
      "INTERNAL_ERROR, IO", "METADATA_ERROR, IO", "OK, IO"
  })
  void translatesKnownFailures(ResultCodes result, NfsStatus expected) {
    for (LookupContext context : LookupContext.values()) {
      assertEquals(expected, NfsStatus.fromOmException(new OMException("Untrusted error message", result), context));
    }
  }

  @ParameterizedTest
  @EnumSource(ResultCodes.class)
  void noOmExceptionBecomesSuccessOrAnNfsSessionError(ResultCodes result) {
    for (LookupContext context : LookupContext.values()) {
      int code = NfsStatus.fromOmException(new OMException(result), context).getCode();
      assertNotEquals(0, code);
      assertNotEquals(10011, code, "Ozone credential expiry must not be reported as NFS lease expiry");
      assertNotEquals(10025, code, "Ganesha owns NFS state IDs");
    }
  }

  @ParameterizedTest
  @CsvSource({
      "NOENT, 2", "IO, 5", "ACCESS, 13", "EXIST, 17", "ISDIR, 21", "INVAL, 22", "NOTEMPTY, 66",
      "DQUOT, 69", "STALE, 70", "BADHANDLE, 10001", "NOTSUPP, 10004", "DELAY, 10008", "FILE_OPEN, 10046"
  })
  void usesProtocolValuesInsteadOfEnumOrdinals(NfsStatus status, int expected) {
    assertEquals(expected, status.getCode());
  }
}
