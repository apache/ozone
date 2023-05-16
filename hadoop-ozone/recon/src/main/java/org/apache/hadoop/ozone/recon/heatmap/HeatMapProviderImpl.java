/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.recon.heatmap;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.EntityMetaData;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

/**
 * This class is to retrieve heatmap data in a specific format for processing.
 */
public class HeatMapProviderImpl implements IHeatMapProvider {
  private OzoneConfiguration ozoneConfiguration;
  private ReconOMMetadataManager omMetadataManager;
  @Override
  public EntityMetaData[] retrieveData(String normalizePath,
                                       String entityType,
                                       String startDate) {
    return new EntityMetaData[0];
  }

  @Override
  public void init(OzoneConfiguration ozoneConf,
                   ReconOMMetadataManager omMetadataMgr,
                   ReconNamespaceSummaryManager namespaceSummaryManager,
                   OzoneStorageContainerManager reconSCM) {
    this.ozoneConfiguration = ozoneConf;
    this.omMetadataManager = omMetadataMgr;
  }
}
