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

package org.apache.hadoop.ozone.recon.heatmap;

import java.net.InetSocketAddress;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.EntityMetaData;
import org.apache.hadoop.ozone.recon.api.types.HealthCheckResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

/**
 * This interface is to provide heatmap data.
 */
public interface IHeatMapProvider {
  /**
   * This method allows heatmap provider to implement fetching of access
   * metadata of entities (volumes/buckets/keys/files) to return data
   * in below desired format for generation of heatmap.
   * List of EntityMetaData objects. Sample EntityMetaData object:
   * entityMetaDataObj:
   * val = "hivevol1676574631/hiveencbuck1676574631/enc_path/hive_tpcds/
   * store_sales/store_sales.dat"
   * readAccessCount = 155074
   *
   * @param path path of entity (volume/bucket/key)
   * @param entityType type of entity (volume/bucket/key)
   * @param startDate the start date since when access metadata to be retrieved
   * @return the list of EntityMetaData objects
   * @throws Exception
   */
  List<EntityMetaData> retrieveData(
      String path,
      String entityType,
      String startDate) throws Exception;

  /**
   * Initializes the config variables and
   * other objects needed by HeatMapProvider.
   * @param ozoneConfiguration
   * @param omMetadataManager
   * @param namespaceSummaryManager
   * @param reconSCM
   * @throws Exception
   */
  void init(OzoneConfiguration ozoneConfiguration,
            ReconOMMetadataManager omMetadataManager,
            ReconNamespaceSummaryManager namespaceSummaryManager,
            OzoneStorageContainerManager reconSCM) throws Exception;

  default InetSocketAddress getSolrAddress() {
    return null;
  }

  default HealthCheckResponse doHeatMapHealthCheck() {
    return new HealthCheckResponse.Builder("Healthy", Response.Status.OK.getStatusCode()).build();
  }
}
