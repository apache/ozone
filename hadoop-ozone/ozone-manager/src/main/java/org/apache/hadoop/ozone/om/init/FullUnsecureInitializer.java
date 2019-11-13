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

package org.apache.hadoop.ozone.om.init;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.ScmClient;

import java.io.IOException;

class FullUnsecureInitializer extends OzoneManagerStorageInitializer {

  FullUnsecureInitializer(OMStorage storage) {
    super(storage);
  }

  @Override
  void initialize(OzoneConfiguration conf) throws IOException {
    final ScmClient client = new ScmClient(conf);
    ScmInfo scmInfo = client.getScmInfo();
    String clusterId = scmInfo.getClusterId();
    String scmId = scmInfo.getScmId();
    if (clusterId == null || clusterId.isEmpty()) {
      throw new IOException("Invalid Cluster ID");
    }
    if (scmId == null || scmId.isEmpty()) {
      throw new IOException("Invalid SCM ID");
    }
    getStorage().setClusterId(clusterId);
    getStorage().setScmId(scmId);
    // NOTE: omId is set automagically from OMStorage and Storage classes,
    // see getNodeProperties, and setNodeProperties for the logic.
    getStorage().initialize();
    System.out.println(
        "OM initialization succeeded. Current cluster id for Storage Directory="
            + getStorage().getStorageDir() + "; Cluster ID=" + getStorage()
            .getClusterID());
  }

}
