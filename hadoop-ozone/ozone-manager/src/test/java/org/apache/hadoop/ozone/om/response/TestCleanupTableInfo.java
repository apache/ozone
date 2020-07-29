/*
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

package org.apache.hadoop.ozone.om.response;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.reflections.Reflections;

import java.io.File;
import java.util.Set;

/**
 * This tests check whether {@link OMClientResponse} have defined
 * {@link CleanupTableInfo} annotation.
 */
public class TestCleanupTableInfo {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void checkAnnotationAndTableName() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(conf);

    Set<String> tables = omMetadataManager.listTableNames();
    Reflections reflections = new Reflections(
        "org.apache.hadoop.ozone.om.response");
    Set<Class<? extends OMClientResponse>> subTypes =
        reflections.getSubTypesOf(OMClientResponse.class);
    subTypes.forEach(aClass -> {
      Assert.assertTrue(aClass + "does not have annotation of" +
              " CleanupTableInfo",
          aClass.isAnnotationPresent(CleanupTableInfo.class));
      String[] cleanupTables =
          aClass.getAnnotation(CleanupTableInfo.class).cleanupTables();
      Assert.assertTrue(cleanupTables.length >=1);
      for (String tableName : cleanupTables) {
        Assert.assertTrue(tables.contains(tableName));
      }
    });
  }
}
