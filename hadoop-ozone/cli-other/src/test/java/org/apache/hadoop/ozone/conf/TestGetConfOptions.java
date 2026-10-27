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

package org.apache.hadoop.ozone.conf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the ozone getconf command.
 */
public class TestGetConfOptions {
  private static GenericTestUtils.PrintStreamCapturer out;
  private static OzoneGetConf subject;

  @BeforeAll
  public static void init() {
    out = GenericTestUtils.captureOut();
    subject = new OzoneGetConf();
    subject.getConf().set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, "1");
    subject.getConf().set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, "service1");
    subject.getConf().set(ScmConfigKeys.OZONE_SCM_NAMES, "localhost");
  }

  @AfterEach
  public void setUp() {
    out.reset();
  }

  @AfterAll
  public static void tearDown() {
    IOUtils.closeQuietly(out);
  }

  @Test
  public void testGetConfWithTheOptionConfKey() {
    subject.run(new String[] {"-confKey", ScmConfigKeys.OZONE_SCM_NAMES});
    assertEquals("localhost\n", out.get());
    out.reset();
    subject.run(new String[] {"confKey", OMConfigKeys.OZONE_OM_NODE_ID_KEY});
    assertEquals("1\n", out.get());
  }

  @Test
  public void testGetConfWithTheOptionStorageContainerManagers() {
    subject.execute(new String[] {"-storagecontainermanagers"});
    assertEquals("localhost\n", out.get());
    out.reset();
    subject.execute(new String[] {"storagecontainermanagers"});
    assertEquals("localhost\n", out.get());
  }

  @Test
  public void testGetConfWithTheOptionOzoneManagers() {
    subject.execute(new String[] {"-ozonemanagers"});
    assertEquals("", out.get());
    out.reset();
    subject.execute(new String[] {"ozonemanagers"});
    assertEquals("", out.get());
  }
}
