/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import java.security.cert.X509Certificate;
import java.util.List;

/**
 * {@link XceiverClientSpi} implementation to work specifically with EC
 * related requests. The only difference at the moment from the basic
 * {@link XceiverClientGrpc} is that this implementation does async calls when
 * a write request is posted via the sendCommandAsync method.
 *
 * @see https://issues.apache.org/jira/browse/HDDS-5954
 */
public class ECXceiverClientGrpc extends XceiverClientGrpc {

  public ECXceiverClientGrpc(
      Pipeline pipeline,
      ConfigurationSource config,
      List<X509Certificate> caCerts) {
    super(pipeline, config, caCerts);
  }

  @Override
  protected boolean shouldBlockAndWaitAsyncReply(
      ContainerProtos.ContainerCommandRequestProto request) {
    return false;
  }
}
