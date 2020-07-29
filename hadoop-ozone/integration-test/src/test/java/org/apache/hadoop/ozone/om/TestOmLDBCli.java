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


import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.debug.DBScanner;
import org.apache.hadoop.ozone.debug.RDBParser;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.ArrayList;


/**
 * This class tests the Debug LDB CLI that reads from an om.db file.
 */
public class TestOmLDBCli {
  private MiniOzoneCluster cluster = null;
  private OzoneConfiguration conf;

  private RDBParser rdbParser;
  private DBScanner dbScanner;
  private DBStore dbStore = null;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    rdbParser = new RDBParser();
    dbScanner = new DBScanner();
  }

  @After
  public void shutdown() throws Exception {
    if (dbStore!=null){
      dbStore.close();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOMDB() throws Exception {
    File newFolder = folder.newFolder();
    if(!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    // Dummy om.db with only keyTable
    dbStore = DBStoreBuilder.newBuilder(conf)
            .setName("om.db")
            .setPath(newFolder.toPath())
            .addTable("keyTable")
            .build();
    // insert 5 keys
    for (int i = 0; i<5; i++) {
      OmKeyInfo value = TestOMRequestUtils.createOmKeyInfo("sampleVol",
          "sampleBuck", "key" + (i+1), HddsProtos.ReplicationType.STAND_ALONE,
              HddsProtos.ReplicationFactor.ONE);
      String key = "key"+ (i);
      Table<byte[], byte[]> keyTable = dbStore.getTable("keyTable");
      keyTable.put(key.getBytes(), value.getProtobuf().toByteArray());
    }
    rdbParser.setDbPath(dbStore.getDbLocation().getAbsolutePath());
    dbScanner.setParent(rdbParser);
    // list will store volumeNames/bucketNames/keyNames
    List<String> keyNames = new ArrayList<>();
    getKeyNames(dbScanner, "keyTable", keyNames);
    Assert.assertEquals(5, keyNames.size());
    Assert.assertTrue(keyNames.contains("key1"));
    Assert.assertTrue(keyNames.contains("key5"));
    Assert.assertFalse(keyNames.contains("key6"));
    dbScanner.setLimit(1);
    getKeyNames(dbScanner, "keyTable", keyNames);
    Assert.assertEquals(1, keyNames.size());
    dbScanner.setLimit(-1);
    getKeyNames(dbScanner, "keyTable", keyNames);
    Assert.assertEquals(0, keyNames.size());
  }

  private static void getKeyNames(DBScanner dbScanner,
      String tableName, List<String> entityNames) throws Exception {
    dbScanner.setTableName(tableName);
    dbScanner.call();
    entityNames.clear();
    Assert.assertFalse(dbScanner.getScannedObjects().isEmpty());
    for (Object o : dbScanner.getScannedObjects()){
      OmKeyInfo keyInfo = (OmKeyInfo)o;
      entityNames.add(keyInfo.getKeyName());
    }
  }
}
