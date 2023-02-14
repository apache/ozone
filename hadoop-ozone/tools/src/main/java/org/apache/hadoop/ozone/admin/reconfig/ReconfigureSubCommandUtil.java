/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.admin.reconfig;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolClientSideTranslatorPB;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Reconfigure subcommand utils.
 */
final class ReconfigureSubCommandUtil {

  private ReconfigureSubCommandUtil() {
  }

  public static ReconfigureProtocol getSingleNodeReconfigureProxy(
      String address) throws IOException {
    OzoneConfiguration ozoneConf = new OzoneConfiguration();
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    InetSocketAddress nodeAddr = NetUtils.createSocketAddr(address);
    return new ReconfigureProtocolClientSideTranslatorPB(
        nodeAddr, user, ozoneConf);
  }

}
