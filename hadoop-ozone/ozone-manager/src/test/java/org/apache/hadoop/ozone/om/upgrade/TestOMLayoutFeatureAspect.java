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

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeatureCatalog.OMLayoutFeature.CREATE_EC;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeatureCatalog.OMLayoutFeature.INITIAL_VERSION;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Class to test annotation based interceptor that checks whether layout
 * feature API is allowed.
 */
public class TestOMLayoutFeatureAspect {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private OzoneConfiguration configuration = new OzoneConfiguration();

  @Before
  public void setUp() throws IOException {
    configuration.set("ozone.metadata.dirs",
        temporaryFolder.newFolder().getAbsolutePath());
  }

  @OMLayoutFeatureAPI(CREATE_EC)
  public String testECMethod() throws Exception {
    return "ec";
  }

  @OMLayoutFeatureAPI(INITIAL_VERSION)
  public String testBasicMethod() throws Exception {
    return "basic";
  }

  @Test
  public void testCheckLayoutFeature() throws Exception {
    OMVersionManager.init(new OMStorage(configuration));
    TestOMLayoutFeatureAspect testObj = new TestOMLayoutFeatureAspect();
    try {
      testObj.testECMethod();
      Assert.fail();
    } catch (OMException ex) {
      assertEquals(NOT_SUPPORTED_OPERATION, ex.getResult());
    }
    String s = testObj.testBasicMethod();
    assertEquals("basic", s);
  }
}