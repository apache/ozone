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

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authorize.AuthorizationException;

/**
 * Backend error categories for the Ozone FSAL. Values follow RFC 8881 section 15.1;
 * Ganesha still owns session errors and the final operation-specific NFS response.
 */
public enum NfsStatus {
  OK(0),
  NOENT(2),
  IO(5),
  ACCESS(13),
  EXIST(17),
  NOTDIR(20),
  ISDIR(21),
  INVAL(22),
  NOTEMPTY(66),
  DQUOT(69),
  STALE(70),
  BADHANDLE(10001),
  BAD_COOKIE(10003),
  BADNAME(10041),
  NOTSUPP(10004),
  DELAY(10008),
  FILE_OPEN(10046);

  private final int code;

  NfsStatus(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  /** Distinguish a missing child name from an object referenced by a persistent handle. */
  public enum LookupContext {
    NAME, HANDLE
  }

  /**
   * Translate an OM failure without inspecting exception message text.
   * The context applies to the lookup that failed, not to the entire NFS compound.
   */
  public static NfsStatus fromOmException(OMException exception, LookupContext context) {
    Objects.requireNonNull(exception, "exception");
    Objects.requireNonNull(context, "context");
    switch (exception.getResult()) {
    case VOLUME_NOT_FOUND:
    case BUCKET_NOT_FOUND:
    case KEY_NOT_FOUND:
    case FILE_NOT_FOUND:
    case DIRECTORY_NOT_FOUND:
      return context == LookupContext.HANDLE ? STALE : NOENT;
    case ACCESS_DENIED:
    case PERMISSION_DENIED:
    case UNAUTHORIZED:
    case USER_MISMATCH:
    case INVALID_AUTH_METHOD:
    case INVALID_TOKEN:
    case TOKEN_EXPIRED:
      // An expired Ozone credential is not an expired NFS open/lock lease.
      return ACCESS;
    case KEY_ALREADY_EXISTS:
    case FILE_ALREADY_EXISTS:
    case DIRECTORY_ALREADY_EXISTS:
      return EXIST;
    case NOT_A_FILE:
      return ISDIR;
    case INVALID_KEY_NAME:
    case INVALID_PATH:
    case INVALID_REQUEST:
      return INVAL;
    case DIRECTORY_NOT_EMPTY:
    case BUCKET_NOT_EMPTY:
      return NOTEMPTY;
    case QUOTA_EXCEEDED:
      return DQUOT;
    case NOT_SUPPORTED_OPERATION:
    case NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION:
    case FEATURE_NOT_ENABLED:
      return NOTSUPP;
    case OM_NOT_INITIALIZED:
    case SCM_IN_SAFE_MODE:
    case KEY_UNDER_LEASE_RECOVERY:
    case KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD:
    case NOT_SUPPORTED_OPERATION_WHEN_PREPARED:
      return DELAY;
    case RENAME_OPEN_FILE:
    case NFS_LEASE_CONFLICT:
      return FILE_OPEN;
    default:
      // Unknown failures, including an exception carrying OK, must not become success.
      return IO;
    }
  }

  /** Translate Hadoop RPC authorization failures without exposing them as server faults. */
  public static NfsStatus fromIOException(IOException exception) {
    for (Throwable cause = Objects.requireNonNull(exception, "exception"); cause != null; cause = cause.getCause()) {
      if (cause instanceof AuthorizationException || cause instanceof AccessControlException) {
        return ACCESS;
      }
      if (cause instanceof RemoteException) {
        IOException unwrapped = ((RemoteException) cause).unwrapRemoteException(
            AuthorizationException.class, AccessControlException.class);
        if (unwrapped instanceof AccessControlException) {
          return ACCESS;
        }
      }
    }
    return IO;
  }
}
