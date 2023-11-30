/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.protocolPB.grpc;

import io.grpc.Context;
import io.grpc.Metadata;

/**
 * Constants to store grpc-client specific header values.
 */
public final class GrpcClientConstants {

  private GrpcClientConstants() {
  }

  public static final Context.Key<String> CLIENT_HOSTNAME_CTX_KEY =
      Context.key("CLIENT_HOSTNAME");

  public static final Metadata.Key<String> CLIENT_HOSTNAME_METADATA_KEY =
      Metadata.Key.of("CLIENT_HOSTNAME", Metadata.ASCII_STRING_MARSHALLER);

  public static final Context.Key<String> CLIENT_IP_ADDRESS_CTX_KEY =
      Context.key("CLIENT_IP_ADDRESS");

  public static final Metadata.Key<String> CLIENT_IP_ADDRESS_METADATA_KEY =
      Metadata.Key.of("CLIENT_IP_ADDRESS", Metadata.ASCII_STRING_MARSHALLER);

}
