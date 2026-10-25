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

package org.apache.hadoop.ozone.om.protocolPB.grpc;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.hadoop.ipc_.RemoteException;

/**
 * Carries an exception thrown by OM from the OM gRPC server to the gRPC client.
 * The exception class and message are sent in the gRPC trailers.
 */
public final class GrpcOmExceptionUtils {
  private static final Metadata.Key<String> EXCEPTION_CLASS_KEY =
      Metadata.Key.of("om-exception-class", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<byte[]> EXCEPTION_MESSAGE_KEY =
      Metadata.Key.of("om-exception-message" + Metadata.BINARY_HEADER_SUFFIX, Metadata.BINARY_BYTE_MARSHALLER);

  private GrpcOmExceptionUtils() {
  }

  /** @return the gRPC error that carries the given exception thrown by OM. */
  public static StatusRuntimeException toStatusRuntimeException(Throwable omException) {
    final Metadata trailers = new Metadata();
    trailers.put(EXCEPTION_CLASS_KEY, omException.getClass().getName());
    if (omException.getMessage() != null) {
      trailers.put(EXCEPTION_MESSAGE_KEY, omException.getMessage().getBytes(UTF_8));
    }
    // NOTE: gRPC clients of old versions read the exception class and message from the description.
    return Status.INTERNAL.withDescription(omException.toString()).asRuntimeException(trailers);
  }

  /**
   * @return the exception thrown by OM that the gRPC error carries,
   *         or null if the error does not come from OM.
   */
  public static RemoteException toRemoteException(StatusRuntimeException e) {
    final Metadata trailers = e.getTrailers();
    final String className = trailers == null ? null : trailers.get(EXCEPTION_CLASS_KEY);
    if (className == null) {
      return null;
    }
    final byte[] message = trailers.get(EXCEPTION_MESSAGE_KEY);
    return new RemoteException(className, message == null ? null : new String(message, UTF_8));
  }
}
