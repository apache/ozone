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

import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.EntityReadAccessHeatMapResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HEATMAP_PROVIDER_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * This class is an implementation of abstract class for retrieving
 * data through HeatMapService.
 */
public class HeatMapServiceImpl extends HeatMapService {
  private static final Logger LOG =
      LoggerFactory.getLogger(HeatMapServiceImpl.class);
  private final OzoneConfiguration ozoneConfiguration;
  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final ReconOMMetadataManager omMetadataManager;
  private final OzoneStorageContainerManager reconSCM;
  private IHeatMapProvider heatMapProvider;

  @Inject
  public HeatMapServiceImpl(OzoneConfiguration ozoneConfiguration,
                            ReconNamespaceSummaryManager
                                     namespaceSummaryManager,
                            ReconOMMetadataManager omMetadataManager,
                            OzoneStorageContainerManager reconSCM)
      throws Exception {
    this.ozoneConfiguration = ozoneConfiguration;
    this.reconNamespaceSummaryManager = namespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
    this.reconSCM = reconSCM;
    initializeProvider();
  }

  private void initializeProvider() throws Exception {
    String heatMapProviderCls = ozoneConfiguration.get(
        OZONE_RECON_HEATMAP_PROVIDER_KEY);
    LOG.info("HeatMapProvider: {}", heatMapProviderCls);
    if (StringUtils.isEmpty(heatMapProviderCls)) {
      heatMapProvider = new HeatMapProviderImpl();
    } else {
      IHeatMapProvider iHeatMapProvider =
          ReconUtils.loadHeatMapProvider(heatMapProviderCls);
      if (null != iHeatMapProvider) {
        iHeatMapProvider.init(ozoneConfiguration, omMetadataManager,
            reconNamespaceSummaryManager, reconSCM);
      } else {
        LOG.warn("HeatMapProvider implementation failed to load !!!");
      }
    }
  }

  @Override
  public EntityReadAccessHeatMapResponse retrieveData(
      String path,
      String entityType,
      String startDate) throws Exception {
    HeatMapUtil heatMapUtil = new HeatMapUtil(
        reconNamespaceSummaryManager, omMetadataManager, reconSCM,
        ozoneConfiguration);
    return heatMapUtil.retrieveData(heatMapProvider, validatePath(path),
        entityType, startDate);
  }

  private String validatePath(String path) {
    if (null != path && path.startsWith(OM_KEY_PREFIX)) {
      path = path.substring(1);
    }
    return path;
  }
}
