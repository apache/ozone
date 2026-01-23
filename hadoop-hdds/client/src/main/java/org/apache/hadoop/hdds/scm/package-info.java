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

/**
 * Classes for different type of container service client.
 *
 * In Ozone there are two type of clients, the Ratis client and the Standalone
 * client.
 *
 * The Ratis client we use for writing data, and it is using Ratis communication
 * facilities to connect to the server side.
 *
 * The Standalone client is used for reading data via protobuf messages over
 * gRPC facilities. For more information on how the gRPC client works, and why
 * one can check the README file in this package.
 *
 * We are using a caching mechanism to cache the created clients inside the
 * {@link org.apache.hadoop.hdds.scm.XceiverClientManager}, and the client
 * interface is defined by the
 * {@link org.apache.hadoop.hdds.scm.XceiverClientSpi} interface.
 */
package org.apache.hadoop.hdds.scm;
