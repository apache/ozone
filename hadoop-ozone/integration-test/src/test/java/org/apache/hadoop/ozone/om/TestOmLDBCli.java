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
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.debug.DBScanner;
import org.apache.hadoop.ozone.debug.RDBParser;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;

/**
 * This class tests the Debug LDB CLI that reads from an om.db file.
 */
public class TestOmLDBCli {
  private MiniOzoneCluster cluster = null;
  private OzoneConfiguration conf;

  private RDBParser rdbParser;
  private DBScanner dbScanner;

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    String volumeName0 = "volume10";
    String bucketName0 = "bucket10";
    OzoneBucket bucket0 = TestDataUtil.createVolumeAndBucket(cluster,
            volumeName0, bucketName0);
    String volumeName1 = "volume11";
    String bucketName1 = "bucket11";
    OzoneBucket bucket1 = TestDataUtil.createVolumeAndBucket(cluster,
            volumeName1, bucketName1);
    String keyName0 = "key0";
    TestDataUtil.createKey(bucket0, keyName0, "");
    String keyName1 = "key1";
    TestDataUtil.createKey(bucket1, keyName1, "");
    cluster.getOzoneManager().stop();
    cluster.getStorageContainerManager().stop();
    rdbParser = new RDBParser();
    dbScanner = new DBScanner();
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOMDB() throws Exception {
    String dbRootPath = conf.get(HddsConfigKeys.OZONE_METADATA_DIRS);
    String dbPath = dbRootPath + "/" + OM_DB_NAME;
    rdbParser.setDbPath(dbPath);
    dbScanner.setParent(rdbParser);
    // list will store volumeNames/bucketNames/keyNames
    List<String> entityNames = new ArrayList<>();
    getEntityNames(dbScanner, "volumeTable", entityNames);
    Assert.assertTrue(entityNames.contains("volume10"));
    Assert.assertTrue(entityNames.contains("volume11"));
    getEntityNames(dbScanner, "bucketTable", entityNames);
    Assert.assertTrue(entityNames.contains("bucket10"));
    Assert.assertTrue(entityNames.contains("bucket11"));
    getEntityNames(dbScanner, "keyTable", entityNames);
    Assert.assertTrue(entityNames.contains("key0"));
    Assert.assertTrue(entityNames.contains("key1"));
    //test maxLimit
    Assert.assertEquals(2, entityNames.size());
    dbScanner.setLimit(1);
    getEntityNames(dbScanner, "keyTable", entityNames);
    Assert.assertEquals(1, entityNames.size());
    dbScanner.setLimit(-1);
    getEntityNames(dbScanner, "keyTable", entityNames);
    Assert.assertEquals(0, entityNames.size());
  }

  private static void getEntityNames(DBScanner dbScanner,
      String tableName, List<String> entityNames) throws Exception {
    dbScanner.setTableName(tableName);
    dbScanner.call();
    entityNames.clear();
    Assert.assertFalse(dbScanner.getScannedObjects().isEmpty());
    for (Object o : dbScanner.getScannedObjects()){
      if(o instanceof OmVolumeArgs) {
        OmVolumeArgs volumeArgs = (OmVolumeArgs) o;
        entityNames.add(volumeArgs.getVolume());
      } else if (o instanceof OmBucketInfo){
        OmBucketInfo bucketInfo = (OmBucketInfo)o;
        entityNames.add(bucketInfo.getBucketName());
      } else {
        OmKeyInfo keyInfo = (OmKeyInfo)o;
        entityNames.add(keyInfo.getKeyName());
      }
    }
  }
}
