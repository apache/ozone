/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.util;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcException;
import org.apache.hadoop.ipc.RpcNoSuchMethodException;
import org.apache.hadoop.ipc.RpcNoSuchProtocolException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.SecretManager;

/**
 * Common Util class for SCM, OM and other components.
 */
public final class OzoneUtils {

  /**
   * Unwrap exception to check if it is some kind of access control problem
   * ({@link AccessControlException} or {@link SecretManager.InvalidToken})
   * or a RpcException.
   */
  public static Throwable getUnwrappedException(Exception ex) {
    if (ex instanceof ServiceException) {
      Throwable t = ex.getCause();
      if (t instanceof RemoteException) {
        t = ((RemoteException) t).unwrapRemoteException();
      }
      while (t != null) {
        if (t instanceof RpcException ||
            t instanceof AccessControlException ||
            t instanceof SecretManager.InvalidToken) {
          return t;
        }
        t = t.getCause();
      }
    }
    return null;
  }

  /**
   * For some Rpc Exceptions, client should not failover.
   */
  public static boolean shouldNotFailoverOnRpcException(Throwable exception) {
    if (exception instanceof RpcException) {
      // Should not failover for following exceptions
      if (exception instanceof RpcNoSuchMethodException ||
          exception instanceof RpcNoSuchProtocolException ||
          exception instanceof RPC.VersionMismatch) {
        return true;
      }
      if (exception.getMessage().contains(
          "RPC response exceeds maximum data length") ||
          exception.getMessage().contains("RPC response has invalid length")) {
        return true;
      }
    }
    return false;
  }
}
