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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_STORAGE_DIR;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.ozone.recon.ReconUtils;

/**
 * Recon's extension of SCMStorageConfig.
 */
public class ReconStorageConfig extends SCMStorageConfig {

  public ReconStorageConfig(OzoneConfiguration conf) throws IOException {
    super(NodeType.RECON, ReconUtils.getReconScmDbDir(conf), RECON_STORAGE_DIR);
  }
  
}
