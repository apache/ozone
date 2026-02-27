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

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFollowerReadFailoverProxyProvider;
import org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;

/** Utilities for testing {@link OzoneManager}. */
public interface OmTestUtil {
  static HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> getFailoverProxyProvider(ObjectStore store) {
    OzoneManagerProtocolClientSideTranslatorPB ozoneManagerClient =
        (OzoneManagerProtocolClientSideTranslatorPB) store.getClientProxy().getOzoneManagerClient();
    
    Hadoop3OmTransport transport =
        (Hadoop3OmTransport) ozoneManagerClient.getTransport();
    return transport.getOmFailoverProxyProvider();
  }

  static HadoopRpcOMFollowerReadFailoverProxyProvider getFollowerReadFailoverProxyProvider(
      ObjectStore store) {
    OzoneManagerProtocolClientSideTranslatorPB ozoneManagerClient =
        (OzoneManagerProtocolClientSideTranslatorPB) store.getClientProxy().getOzoneManagerClient();

    Hadoop3OmTransport transport =
        (Hadoop3OmTransport) ozoneManagerClient.getTransport();
    return transport.getOmFollowerReadFailoverProxyProvider();
  }

  static String getCurrentOmProxyNodeId(ObjectStore store) {
    return getFailoverProxyProvider(store).getCurrentProxyOMNodeId();
  }
}
