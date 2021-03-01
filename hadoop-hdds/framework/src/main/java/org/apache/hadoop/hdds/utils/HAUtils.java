/**
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.utils;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.proxy.SCMBlockLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.tracing.TracingUtil;

import java.io.IOException;

/**
 * utility class used by SCM and OM for HA.
 */
public final class HAUtils {

  private HAUtils() {
  }

  public static ScmInfo getScmInfo(OzoneConfiguration conf)
      throws IOException {
    try {
      return getScmBlockClient(conf).getScmInfo();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to get SCM info", e);
    }
  }

  /**
   * Add SCM to the cluster.
   * @param conf - OzoneConfiguration
   * @param request - AddSCMRequest which has details of SCM to be added.
   * @param selfId - Node Id of the SCM which is submitting the request to
   * add SCM.
   * @return true - if SCM node is added successfully, else false.
   * @throws IOException
   */
  public static boolean addSCM(OzoneConfiguration conf, AddSCMRequest request,
      String selfId) throws IOException {
    OzoneConfiguration config = SCMHAUtils.removeSelfId(conf, selfId);
    try {
      return getScmBlockClient(config).addSCM(request);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to add SCM", e);
    }
  }

  /**
   * Create a scm block client.
   *
   * @return {@link ScmBlockLocationProtocol}
   * @throws IOException
   */
  public static ScmBlockLocationProtocol getScmBlockClient(
      OzoneConfiguration conf) throws IOException {
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            new SCMBlockLocationFailoverProxyProvider(conf));
    return TracingUtil
        .createProxy(scmBlockLocationClient, ScmBlockLocationProtocol.class,
            conf);
  }
}
