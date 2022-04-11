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
package org.apache.hadoop.ozone.conf;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Tests the ozone getconf command.
 */
public class TestGetConfOptions {
  private static OzoneConfiguration conf;
  private static ByteArrayOutputStream bout;
  private static PrintStream psBackup;
  private static final String DEFAULT_ENCODING = UTF_8.name();

  @BeforeClass
  public static void init() throws UnsupportedEncodingException {
    conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, "1");
    conf.set(OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY, "service1");
    conf.set(ScmConfigKeys.OZONE_SCM_NAMES, "localhost");
    psBackup = System.out;
    bout = new ByteArrayOutputStream();
    PrintStream psOut = new PrintStream(bout, false, DEFAULT_ENCODING);
    System.setOut(psOut);
  }

  @After
  public void setUp(){
    bout.reset();
  }

  @AfterClass
  public static void tearDown(){
    System.setOut(psBackup);
  }

  @Test
  public void testGetConfWithTheOptionConfKey()
      throws UnsupportedEncodingException {
    new OzoneGetConf(conf)
        .run(new String[] {"-confKey", ScmConfigKeys.OZONE_SCM_NAMES});
    Assert.assertEquals("localhost\n", bout.toString(DEFAULT_ENCODING));
    bout.reset();
    new OzoneGetConf(conf)
        .run(new String[] {"confKey", OMConfigKeys.OZONE_OM_NODE_ID_KEY});
    Assert.assertEquals("1\n", bout.toString(DEFAULT_ENCODING));
  }

  @Test
  public void testGetConfWithTheOptionStorageContainerManagers()
      throws UnsupportedEncodingException {
    new OzoneGetConf(conf).run(new String[] {"-storagecontainermanagers"});
    Assert.assertEquals("localhost\n", bout.toString(DEFAULT_ENCODING));
    bout.reset();
    new OzoneGetConf(conf).run(new String[] {"storagecontainermanagers"});
    Assert.assertEquals("localhost\n", bout.toString(DEFAULT_ENCODING));
  }

  @Test
  public void testGetConfWithTheOptionOzoneManagers()
      throws UnsupportedEncodingException {
    new OzoneGetConf(conf).run(new String[] {"-ozonemanagers"});
    Assert.assertEquals("{service1=[]}\n", bout.toString(DEFAULT_ENCODING));
    bout.reset();
    new OzoneGetConf(conf).run(new String[] {"ozonemanagers"});
    Assert.assertEquals("{service1=[]}\n", bout.toString(DEFAULT_ENCODING));
  }
}
