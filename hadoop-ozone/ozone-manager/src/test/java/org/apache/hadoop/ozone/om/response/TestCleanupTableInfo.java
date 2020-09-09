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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.reflections.Reflections;

import java.io.File;
import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * This tests check whether {@link OMClientResponse} have defined
 * {@link CleanupTableInfo} annotation.
 */
public class TestCleanupTableInfo {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneConfiguration conf = new OzoneConfiguration();

  @Before
  public void setupMetaManager() throws Exception {
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
  }

  @Test
  public void checkAllWriteResponseHasCleanupTableAnnotation() {
    getResponseClasses().forEach(aClass -> {
      Assert.assertTrue(
          aClass + "does not have annotation of CleanupTableInfo",
          aClass.isAnnotationPresent(CleanupTableInfo.class));
    });
  }

  @Test
  public void checkWriteResponseIsAnnotatedWithKnownTableNames()
      throws Exception {
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(conf);
    Set<String> tables = omMetadataManager.listTableNames();

    getResponseClasses().forEach(aClass -> {

      CleanupTableInfo annotation =
          aClass.getAnnotation(CleanupTableInfo.class);
      String[] cleanupTables = annotation.cleanupTables();
      boolean cleanupAll = annotation.cleanupAll();

      if (cleanupTables.length >= 1) {
        assertTrue(
            Arrays.stream(cleanupTables).allMatch(tables::contains)
        );
      } else {
        assertTrue(cleanupAll);
      }

    });
  }

  private Set<Class<? extends OMClientResponse>> getResponseClasses() {
    Reflections reflections =
        new Reflections("org.apache.hadoop.ozone.om.response");
    return reflections.getSubTypesOf(OMClientResponse.class);
  }
}
