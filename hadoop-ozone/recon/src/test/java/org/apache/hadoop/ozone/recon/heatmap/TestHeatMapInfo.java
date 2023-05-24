/**
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

package org.apache.hadoop.ozone.recon.heatmap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.EntityMetaData;
import org.apache.hadoop.ozone.recon.api.types.EntityReadAccessHeatMapResponse;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.mockito.Mockito.mock;

/**
 * This class test heatmap provider's data to be consumed
 * and used for generating heatmap.
 */
public class TestHeatMapInfo {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private boolean isSetupDone = false;
  private ReconOMMetadataManager reconOMMetadataManager;
  private String auditRespStr;
  private HeatMapServiceImpl heatMapService;

  @SuppressWarnings("checkstyle:methodlength")
  private void initializeInjector() throws Exception {
    reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(temporaryFolder.newFolder()),
        temporaryFolder.newFolder());

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder)
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            // No longer using mock reconSCM as we need nodeDB in Facade
            //  to establish datanode UUID to hostname mapping
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .withContainerDB()
            .addBinding(StorageContainerServiceProvider.class,
                mock(StorageContainerServiceProviderImpl.class))
            .addBinding(ContainerHealthSchemaManager.class)
            .build();
    heatMapService = reconTestInjector.getInstance(HeatMapServiceImpl.class);
    auditRespStr = "{\n" +
        "  \"responseHeader\": {\n" +
        "    \"zkConnected\": true,\n" +
        "    \"status\": 0,\n" +
        "    \"QTime\": 1446,\n" +
        "    \"params\": {\n" +
        "      \"q\": \"*:*\",\n" +
        "      \"json.facet\": \"{\\n    resources:{\\n      type : terms" +
        ",\\n      field : resource,\\n      sort : " +
        "\\\"read_access_count desc\\\",\\n      limit : 100,\\n      " +
        "facet:{\\n        read_access_count : \\\"sum(event_count)\\\"\\n " +
        "     }\\n    }\\n  }\",\n" +
        "      \"fl\": \"access, agent, repo, resource, resType, " +
        "event_count\",\n" +
        "      \"start\": \"0\",\n" +
        "      \"fq\": [\n" +
        "        \"access:read\",\n" +
        "        \"repo:cm_ozone\",\n" +
        "        \"resType:key\",\n" +
        "        \"evtTime:[2023-02-02T18:30:00Z TO NOW]\"\n" +
        "      ],\n" +
        "      \"sort\": \"event_count desc\",\n" +
        "      \"rows\": \"0\",\n" +
        "      \"wt\": \"json\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"response\": {\n" +
        "    \"numFound\": 2324814,\n" +
        "    \"start\": 0,\n" +
        "    \"docs\": []\n" +
        "  },\n" +
        "  \"facets\": {\n" +
        "    \"count\": 2324814,\n" +
        "    \"resources\": {\n" +
        "      \"buckets\": [\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/store_sales/store_sales.dat\",\n" +
        "          \"count\": 60412,\n" +
        "          \"read_access_count\": 155074\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/reg_path/" +
        "hive_tpcds/store_sales/store_sales.dat\",\n" +
        "          \"count\": 59880,\n" +
        "          \"read_access_count\": 155069\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hivebuck1675429570/reg_path/" +
        "hive_tpcds/store_sales/store_sales.dat\",\n" +
        "          \"count\": 50677,\n" +
        "          \"read_access_count\": 129977\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hiveencbuck1675429570/" +
        "enc_path/hive_tpcds/store_sales/store_sales.dat\",\n" +
        "          \"count\": 49630,\n" +
        "          \"read_access_count\": 128525\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hivebuck1676910931/reg_path/" +
        "hive_tpcds/store_sales/store_sales.dat\",\n" +
        "          \"count\": 42485,\n" +
        "          \"read_access_count\": 110185\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hiveencbuck1676910931/" +
        "enc_path/hive_tpcds/store_sales/store_sales.dat\",\n" +
        "          \"count\": 43732,\n" +
        "          \"read_access_count\": 109463\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/catalog_sales/catalog_sales.dat\",\n" +
        "          \"count\": 33773,\n" +
        "          \"read_access_count\": 68567\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/reg_path/" +
        "hive_tpcds/catalog_sales/catalog_sales.dat\",\n" +
        "          \"count\": 33666,\n" +
        "          \"read_access_count\": 68566\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hivebuck1675429570/reg_path/" +
        "hive_tpcds/catalog_sales/catalog_sales.dat\",\n" +
        "          \"count\": 26914,\n" +
        "          \"read_access_count\": 57481\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hiveencbuck1675429570/" +
        "enc_path/hive_tpcds/catalog_sales/catalog_sales.dat\",\n" +
        "          \"count\": 26463,\n" +
        "          \"read_access_count\": 56950\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676533485/hiveencbuck1676533485/" +
        "enc_path/hive_tpcds/store_sales/store_sales.dat\",\n" +
        "          \"count\": 20153,\n" +
        "          \"read_access_count\": 52456\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676533485/hivebuck1676533485/reg_path/" +
        "hive_tpcds/store_sales/store_sales.dat\",\n" +
        "          \"count\": 20386,\n" +
        "          \"read_access_count\": 52450\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hivebuck1676910931/reg_path/" +
        "hive_tpcds/catalog_sales/catalog_sales.dat\",\n" +
        "          \"count\": 23395,\n" +
        "          \"read_access_count\": 48869\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hiveencbuck1676910931/" +
        "enc_path/hive_tpcds/catalog_sales/catalog_sales.dat\",\n" +
        "          \"count\": 24122,\n" +
        "          \"read_access_count\": 48397\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivewritevol1676574237/hivebucket1676574242/" +
        "hive_write/vectortab_txt/delta_0000001_0000001_0000/vectortab\",\n" +
        "          \"count\": 8723,\n" +
        "          \"read_access_count\": 37446\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/reg_path/" +
        "hive_tpcds/web_sales/web_sales.dat\",\n" +
        "          \"count\": 21559,\n" +
        "          \"read_access_count\": 36119\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/web_sales/web_sales.dat\",\n" +
        "          \"count\": 21474,\n" +
        "          \"read_access_count\": 36114\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675358593/hiveencbuck1675358593/" +
        "enc_path/hive_tpcds/store_sales/store_sales.dat\",\n" +
        "          \"count\": 13447,\n" +
        "          \"read_access_count\": 34205\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivewritevol1675429188/hivebucket1675429193/" +
        "hive_write/vectortab_txt/delta_0000001_0000001_0000/vectortab\",\n" +
        "          \"count\": 7413,\n" +
        "          \"read_access_count\": 32246\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675358593/hivebuck1675358593/reg_path/" +
        "hive_tpcds/store_sales/store_sales.dat\",\n" +
        "          \"count\": 12876,\n" +
        "          \"read_access_count\": 32047\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hivebuck1675429570/reg_path/" +
        "hive_tpcds/web_sales/web_sales.dat\",\n" +
        "          \"count\": 18205,\n" +
        "          \"read_access_count\": 30280\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hiveencbuck1675429570/" +
        "enc_path/hive_tpcds/web_sales/web_sales.dat\",\n" +
        "          \"count\": 18293,\n" +
        "          \"read_access_count\": 29936\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivewritevol1676910511/hivebucket1676910516/" +
        "hive_write/vectortab_txt/delta_0000001_0000001_0000/vectortab\",\n" +
        "          \"count\": 6188,\n" +
        "          \"read_access_count\": 26095\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hivebuck1676910931/reg_path/" +
        "hive_tpcds/web_sales/web_sales.dat\",\n" +
        "          \"count\": 15591,\n" +
        "          \"read_access_count\": 25685\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hiveencbuck1676910931/" +
        "enc_path/hive_tpcds/web_sales/web_sales.dat\",\n" +
        "          \"count\": 16501,\n" +
        "          \"read_access_count\": 25514\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676533485/hivebuck1676533485/reg_path/" +
        "hive_tpcds/catalog_sales/catalog_sales.dat\",\n" +
        "          \"count\": 10884,\n" +
        "          \"read_access_count\": 23193\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676533485/hiveencbuck1676533485/" +
        "enc_path/hive_tpcds/catalog_sales/catalog_sales.dat\",\n" +
        "          \"count\": 11350,\n" +
        "          \"read_access_count\": 23190\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675358593/hiveencbuck1675358593/" +
        "enc_path/hive_tpcds/catalog_sales/catalog_sales.dat\",\n" +
        "          \"count\": 7198,\n" +
        "          \"read_access_count\": 15126\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675358593/hivebuck1675358593/reg_path/" +
        "hive_tpcds/catalog_sales/catalog_sales.dat\",\n" +
        "          \"count\": 7047,\n" +
        "          \"read_access_count\": 14151\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676900292/hivebuck1676900292/reg_path/" +
        "hive_tpcds/store_sales/store_sales.dat\",\n" +
        "          \"count\": 5040,\n" +
        "          \"read_access_count\": 13692\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676900292/hiveencbuck1676900292/" +
        "enc_path/hive_tpcds/store_sales/store_sales.dat\",\n" +
        "          \"count\": 5080,\n" +
        "          \"read_access_count\": 13681\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivewritevol1676533084/hivebucket1676533089/" +
        "hive_write/vectortab_txt/delta_0000001_0000001_0000/vectortab\",\n" +
        "          \"count\": 3063,\n" +
        "          \"read_access_count\": 12976\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676533485/hivebuck1676533485/reg_path/" +
        "hive_tpcds/web_sales/web_sales.dat\",\n" +
        "          \"count\": 7581,\n" +
        "          \"read_access_count\": 12223\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676533485/hiveencbuck1676533485/" +
        "enc_path/hive_tpcds/web_sales/web_sales.dat\",\n" +
        "          \"count\": 7551,\n" +
        "          \"read_access_count\": 12217\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/" +
        "reg_path/hive_tpcds/date_dim\",\n" +
        "          \"count\": 6083,\n" +
        "          \"read_access_count\": 9047\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/date_dim\",\n" +
        "          \"count\": 6097,\n" +
        "          \"read_access_count\": 9047\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/reg_path/" +
        "hive_tpcds/date_dim/date_dim.dat\",\n" +
        "          \"count\": 8516,\n" +
        "          \"read_access_count\": 9044\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/date_dim/date_dim.dat\",\n" +
        "          \"count\": 8545,\n" +
        "          \"read_access_count\": 9044\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivewritevol1675358193/hivebucket1675358197/" +
        "hive_write/vectortab_txt/delta_0000001_0000001_0000/vectortab\",\n" +
        "          \"count\": 1753,\n" +
        "          \"read_access_count\": 8034\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675358593/hiveencbuck1675358593/" +
        "enc_path/hive_tpcds/web_sales/web_sales.dat\",\n" +
        "          \"count\": 5081,\n" +
        "          \"read_access_count\": 7975\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hivebuck1675429570/reg_path/" +
        "hive_tpcds/date_dim\",\n" +
        "          \"count\": 4969,\n" +
        "          \"read_access_count\": 7584\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hivebuck1675429570/reg_path/" +
        "hive_tpcds/date_dim/date_dim.dat\",\n" +
        "          \"count\": 7222,\n" +
        "          \"read_access_count\": 7581\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675358593/hivebuck1675358593/reg_path/" +
        "hive_tpcds/web_sales/web_sales.dat\",\n" +
        "          \"count\": 4409,\n" +
        "          \"read_access_count\": 7513\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hiveencbuck1675429570/" +
        "enc_path/hive_tpcds/date_dim\",\n" +
        "          \"count\": 4876,\n" +
        "          \"read_access_count\": 7494\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hiveencbuck1675429570/" +
        "enc_path/hive_tpcds/date_dim/date_dim.dat\",\n" +
        "          \"count\": 7078,\n" +
        "          \"read_access_count\": 7486\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/reg_path/" +
        "hive_tpcds/store_sales\",\n" +
        "          \"count\": 4654,\n" +
        "          \"read_access_count\": 6463\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/store_sales\",\n" +
        "          \"count\": 4648,\n" +
        "          \"read_access_count\": 6463\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hivebuck1676910931/reg_path" +
        "/hive_tpcds/date_dim\",\n" +
        "          \"count\": 4323,\n" +
        "          \"read_access_count\": 6421\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hivebuck1676910931/reg_path/" +
        "hive_tpcds/date_dim/date_dim.dat\",\n" +
        "          \"count\": 6085,\n" +
        "          \"read_access_count\": 6418\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hiveencbuck1676910931/" +
        "enc_path/hive_tpcds/date_dim\",\n" +
        "          \"count\": 4311,\n" +
        "          \"read_access_count\": 6387\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hiveencbuck1676910931/" +
        "enc_path/hive_tpcds/date_dim/date_dim.dat\",\n" +
        "          \"count\": 6030,\n" +
        "          \"read_access_count\": 6384\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676900292/hiveencbuck1676900292/" +
        "enc_path/hive_tpcds/catalog_sales/catalog_sales.dat\",\n" +
        "          \"count\": 2801,\n" +
        "          \"read_access_count\": 6049\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676900292/hivebuck1676900292/" +
        "reg_path/hive_tpcds/catalog_sales/catalog_sales.dat\",\n" +
        "          \"count\": 2765,\n" +
        "          \"read_access_count\": 6048\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/" +
        "reg_path/hive_tpcds/inventory/inventory.dat\",\n" +
        "          \"count\": 3403,\n" +
        "          \"read_access_count\": 5712\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/inventory/inventory.dat\",\n" +
        "          \"count\": 3374,\n" +
        "          \"read_access_count\": 5712\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/" +
        "reg_path/hive_tpcds/customer_demographics/" +
        "customer_demographics.dat\",\n" +
        "          \"count\": 4648,\n" +
        "          \"read_access_count\": 5440\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/customer_demographics/" +
        "customer_demographics.dat\",\n" +
        "          \"count\": 4429,\n" +
        "          \"read_access_count\": 5440\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hivebuck1675429570/" +
        "reg_path/hive_tpcds/store_sales\",\n" +
        "          \"count\": 3821,\n" +
        "          \"read_access_count\": 5418\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hiveencbuck1675429570/" +
        "enc_path/hive_tpcds/store_sales\",\n" +
        "          \"count\": 3763,\n" +
        "          \"read_access_count\": 5357\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hivebuck1675429570/" +
        "reg_path/hive_tpcds/inventory/inventory.dat\",\n" +
        "          \"count\": 3021,\n" +
        "          \"read_access_count\": 4788\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hiveencbuck1675429570/" +
        "enc_path/hive_tpcds/inventory/inventory.dat\",\n" +
        "          \"count\": 3025,\n" +
        "          \"read_access_count\": 4718\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/" +
        "reg_path/hive_tpcds/item\",\n" +
        "          \"count\": 3638,\n" +
        "          \"read_access_count\": 4627\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/item\",\n" +
        "          \"count\": 3651,\n" +
        "          \"read_access_count\": 4627\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/" +
        "reg_path/hive_tpcds/item/item.dat\",\n" +
        "          \"count\": 4494,\n" +
        "          \"read_access_count\": 4624\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/item/item.dat\",\n" +
        "          \"count\": 4485,\n" +
        "          \"read_access_count\": 4624\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hivebuck1676910931/" +
        "reg_path/hive_tpcds/store_sales\",\n" +
        "          \"count\": 3306,\n" +
        "          \"read_access_count\": 4593\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hiveencbuck1676910931/" +
        "enc_path/hive_tpcds/store_sales\",\n" +
        "          \"count\": 3300,\n" +
        "          \"read_access_count\": 4563\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hivebuck1675429570/reg_path/" +
        "hive_tpcds/customer_demographics/customer_demographics.dat\",\n" +
        "          \"count\": 3752,\n" +
        "          \"read_access_count\": 4560\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hiveencbuck1675429570/" +
        "enc_path/hive_tpcds/customer_demographics/" +
        "customer_demographics.dat\",\n" +
        "          \"count\": 3653,\n" +
        "          \"read_access_count\": 4510\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hivebuck1676910931/reg_path/" +
        "hive_tpcds/inventory/inventory.dat\",\n" +
        "          \"count\": 2348,\n" +
        "          \"read_access_count\": 4046\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hiveencbuck1676910931/" +
        "enc_path/hive_tpcds/inventory/inventory.dat\",\n" +
        "          \"count\": 2477,\n" +
        "          \"read_access_count\": 4032\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/" +
        "reg_path/hive_tpcds/web_sales\",\n" +
        "          \"count\": 2595,\n" +
        "          \"read_access_count\": 4015\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/web_sales\",\n" +
        "          \"count\": 2590,\n" +
        "          \"read_access_count\": 4015\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hivebuck1675429570/" +
        "reg_path/hive_tpcds/item\",\n" +
        "          \"count\": 3004,\n" +
        "          \"read_access_count\": 3879\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hivebuck1675429570/" +
        "reg_path/hive_tpcds/item/item.dat\",\n" +
        "          \"count\": 3784,\n" +
        "          \"read_access_count\": 3876\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hivebuck1676910931/" +
        "reg_path/hive_tpcds/customer_demographics/" +
        "customer_demographics.dat\",\n" +
        "          \"count\": 3173,\n" +
        "          \"read_access_count\": 3865\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hiveencbuck1676910931/" +
        "enc_path/hive_tpcds/customer_demographics/" +
        "customer_demographics.dat\",\n" +
        "          \"count\": 3168,\n" +
        "          \"read_access_count\": 3840\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hiveencbuck1675429570/" +
        "enc_path/hive_tpcds/item\",\n" +
        "          \"count\": 2959,\n" +
        "          \"read_access_count\": 3831\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hiveencbuck1675429570/" +
        "enc_path/hive_tpcds/item/item.dat\",\n" +
        "          \"count\": 3736,\n" +
        "          \"read_access_count\": 3827\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/" +
        "reg_path/hive_tpcds/catalog_sales\",\n" +
        "          \"count\": 2800,\n" +
        "          \"read_access_count\": 3811\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/catalog_sales\",\n" +
        "          \"count\": 2796,\n" +
        "          \"read_access_count\": 3811\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hivebuck1675429570/" +
        "reg_path/hive_tpcds/web_sales\",\n" +
        "          \"count\": 2154,\n" +
        "          \"read_access_count\": 3366\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hiveencbuck1675429570/" +
        "enc_path/hive_tpcds/web_sales\",\n" +
        "          \"count\": 2111,\n" +
        "          \"read_access_count\": 3328\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hivebuck1676910931/reg_path/" +
        "hive_tpcds/item\",\n" +
        "          \"count\": 2586,\n" +
        "          \"read_access_count\": 3283\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hivebuck1676910931/reg_path/" +
        "hive_tpcds/item/item.dat\",\n" +
        "          \"count\": 3179,\n" +
        "          \"read_access_count\": 3280\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hiveencbuck1676910931/" +
        "enc_path/hive_tpcds/item\",\n" +
        "          \"count\": 2572,\n" +
        "          \"read_access_count\": 3267\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676910931/hiveencbuck1676910931/" +
        "enc_path/hive_tpcds/item/item.dat\",\n" +
        "          \"count\": 3168,\n" +
        "          \"read_access_count\": 3264\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hivebuck1675429570/reg_path/" +
        "hive_tpcds/catalog_sales\",\n" +
        "          \"count\": 2315,\n" +
        "          \"read_access_count\": 3195\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676900292/hiveencbuck1676900292/" +
        "enc_path/hive_tpcds/web_sales/web_sales.dat\",\n" +
        "          \"count\": 1941,\n" +
        "          \"read_access_count\": 3188\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676900292/hivebuck1676900292/reg_path/" +
        "hive_tpcds/web_sales/web_sales.dat\",\n" +
        "          \"count\": 1828,\n" +
        "          \"read_access_count\": 3187\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1675429570/hiveencbuck1675429570/" +
        "enc_path/hive_tpcds/catalog_sales\",\n" +
        "          \"count\": 2279,\n" +
        "          \"read_access_count\": 3167\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivewritevol1676899876/hivebucket1676899881/" +
        "hive_write/vectortab_txt/delta_0000001_0000001_0000/vectortab\",\n" +
        "          \"count\": 749,\n" +
        "          \"read_access_count\": 3088\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676533485/hivebuck1676533485/reg_path/" +
        "hive_tpcds/date_dim\",\n" +
        "          \"count\": 2067,\n" +
        "          \"read_access_count\": 3062\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676533485/hiveencbuck1676533485/" +
        "enc_path/hive_tpcds/date_dim\",\n" +
        "          \"count\": 2057,\n" +
        "          \"read_access_count\": 3062\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676533485/hivebuck1676533485/reg_path/" +
        "hive_tpcds/date_dim/date_dim.dat\",\n" +
        "          \"count\": 2902,\n" +
        "          \"read_access_count\": 3059\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676533485/hiveencbuck1676533485/" +
        "enc_path/hive_tpcds/date_dim/date_dim.dat\",\n" +
        "          \"count\": 2896,\n" +
        "          \"read_access_count\": 3059\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/reg_path/" +
        "hive_tpcds/store\",\n" +
        "          \"count\": 2304,\n" +
        "          \"read_access_count\": 2927\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/store\",\n" +
        "          \"count\": 2307,\n" +
        "          \"read_access_count\": 2927\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hivebuck1676574631/reg_path/" +
        "hive_tpcds/store/store.dat\",\n" +
        "          \"count\": 2761,\n" +
        "          \"read_access_count\": 2924\n" +
        "        },\n" +
        "        {\n" +
        "          \"val\": \"hivevol1676574631/hiveencbuck1676574631/" +
        "enc_path/hive_tpcds/store/store.dat\",\n" +
        "          \"count\": 2780,\n" +
        "          \"read_access_count\": 2924\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  }\n" +
        "}";
  }

  @Before
  public void setUp() throws Exception {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();
      isSetupDone = true;
    }
  }

  @Test
  public void testHeatMapGeneratedInfo() throws IOException {
    // Setup
    // Run the test
    JsonElement jsonElement = JsonParser.parseString(auditRespStr);
    JsonObject jsonObject = jsonElement.getAsJsonObject();
    JsonElement facets = jsonObject.get("facets");
    JsonObject facetsBucketsObject =
        facets.getAsJsonObject().get("resources")
            .getAsJsonObject();
    ObjectMapper objectMapper = new ObjectMapper();

    HeatMapProviderDataResource auditLogFacetsResources =
        objectMapper.readValue(
            facetsBucketsObject.toString(), HeatMapProviderDataResource.class);
    EntityMetaData[] entities = auditLogFacetsResources.getMetaDataList();
    List<EntityMetaData> entityMetaDataList =
        Arrays.stream(entities).collect(Collectors.toList());
    EntityReadAccessHeatMapResponse entityReadAccessHeatMapResponse =
        heatMapService.generateHeatMap(entityMetaDataList);
    Assertions.assertTrue(
        entityReadAccessHeatMapResponse.getChildren().size() > 0);
    Assertions.assertEquals(12,
        entityReadAccessHeatMapResponse.getChildren().size());
    Assertions.assertEquals(25600, entityReadAccessHeatMapResponse.
        getSize());
    Assertions.assertEquals(2924, entityReadAccessHeatMapResponse.
        getMinAccessCount());
    Assertions.assertEquals(155074, entityReadAccessHeatMapResponse.
        getMaxAccessCount());
    Assertions.assertEquals("root", entityReadAccessHeatMapResponse.
        getLabel());

  }

}
