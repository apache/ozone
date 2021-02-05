/*
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
package org.apache.hadoop.hdds.conf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test class for OzoneConfiguration.
 */
public class TestOzoneConfiguration {

  private OzoneConfiguration conf;

  @Rule
  public TemporaryFolder tempConfigs = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
  }

  private void startConfig(BufferedWriter out) throws IOException {
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
  }

  private void endConfig(BufferedWriter out) throws IOException {
    out.write("</configuration>\n");
    out.flush();
    out.close();
  }

  @Test
  public void testGetAllPropertiesByTags() throws Exception {
    File coreDefault = tempConfigs.newFile("core-default-test.xml");
    File coreSite = tempConfigs.newFile("core-site-test.xml");
    FileOutputStream coreDefaultStream = new FileOutputStream(coreDefault);
    try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
        coreDefaultStream, StandardCharsets.UTF_8))) {
      startConfig(out);
      appendProperty(out, "hadoop.tags.system", "YARN,HDFS,NAMENODE");
      appendProperty(out, "hadoop.tags.custom", "MYCUSTOMTAG");
      appendPropertyByTag(out, "dfs.cblock.trace.io", "false", "YARN");
      appendPropertyByTag(out, "dfs.replication", "1", "HDFS");
      appendPropertyByTag(out, "dfs.namenode.logging.level", "INFO",
          "NAMENODE");
      appendPropertyByTag(out, "dfs.random.key", "XYZ", "MYCUSTOMTAG");
      endConfig(out);

      Path fileResource = new Path(coreDefault.getAbsolutePath());
      conf.addResource(fileResource);
      Assert.assertEquals("XYZ", conf.getAllPropertiesByTag("MYCUSTOMTAG")
          .getProperty("dfs.random.key"));
    }

    FileOutputStream coreSiteStream = new FileOutputStream(coreSite);
    try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
        coreSiteStream, StandardCharsets.UTF_8))) {
      startConfig(out);
      appendProperty(out, "dfs.random.key", "ABC");
      appendProperty(out, "dfs.replication", "3");
      appendProperty(out, "dfs.cblock.trace.io", "true");
      endConfig(out);

      Path fileResource = new Path(coreSite.getAbsolutePath());
      conf.addResource(fileResource);
    }

    // Test if values are getting overridden even without tags being present
    Assert.assertEquals("3", conf.getAllPropertiesByTag("HDFS")
        .getProperty("dfs.replication"));
    Assert.assertEquals("ABC", conf.getAllPropertiesByTag("MYCUSTOMTAG")
        .getProperty("dfs.random.key"));
    Assert.assertEquals("true", conf.getAllPropertiesByTag("YARN")
        .getProperty("dfs.cblock.trace.io"));
  }

  @Test
  public void getConfigurationObject() {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set("test.scm.client.address", "address");
    ozoneConfig.set("test.scm.client.bind.host", "host");
    ozoneConfig.setBoolean("test.scm.client.enabled", true);
    ozoneConfig.setInt("test.scm.client.port", 5555);
    ozoneConfig.setTimeDuration("test.scm.client.wait", 10, TimeUnit.MINUTES);
    ozoneConfig.set("test.scm.client.class", Integer.class.getName());

    SimpleConfiguration configuration =
        ozoneConfig.getObject(SimpleConfiguration.class);

    Assert.assertEquals("host", configuration.getBindHost());
    Assert.assertEquals("address", configuration.getClientAddress());
    Assert.assertTrue(configuration.isEnabled());
    Assert.assertEquals(5555, configuration.getPort());
    Assert.assertEquals(600, configuration.getWaitTime());
    Assert.assertEquals(Integer.class, configuration.getMyClass());
  }

  @Test
  public void getConfigurationObjectWithDefault() {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();

    SimpleConfiguration configuration =
        ozoneConfiguration.getObject(SimpleConfiguration.class);

    Assert.assertTrue(configuration.isEnabled());
    Assert.assertEquals(9878, configuration.getPort());
    Assert.assertEquals(Object.class, configuration.getMyClass());
  }

  @Test
  public void setConfigFromObject() {
    // GIVEN
    SimpleConfiguration object = new SimpleConfiguration();
    object.setBindHost("host");
    object.setClientAddress("address");
    object.setEnabled(true);
    object.setPort(5555);
    object.setWaitTime(600);
    object.setMyClass(this.getClass());

    OzoneConfiguration subject = new OzoneConfiguration();

    // WHEN
    subject.setFromObject(object);

    // THEN
    Assert.assertEquals(object.getBindHost(),
        subject.get("test.scm.client.bind.host"));
    Assert.assertEquals(object.getClientAddress(),
        subject.get("test.scm.client.address"));
    Assert.assertEquals(object.isEnabled(),
        subject.getBoolean("test.scm.client.enabled", false));
    Assert.assertEquals(object.getPort(),
        subject.getInt("test.scm.client.port", 0));
    Assert.assertEquals(TimeUnit.SECONDS.toMinutes(object.getWaitTime()),
        subject.getTimeDuration("test.scm.client.wait", 0, TimeUnit.MINUTES));
    Assert.assertEquals(this.getClass(),
        subject.getClass("test.scm.client.class", null));
  }

  @Test
  public void setConfigFromObjectWithConfigDefaults() {
    // GIVEN
    OzoneConfiguration subject = new OzoneConfiguration();
    SimpleConfiguration object = subject.getObject(SimpleConfiguration.class);

    // WHEN
    subject.setFromObject(object);

    // THEN
    Assert.assertEquals("0.0.0.0",
        subject.get("test.scm.client.bind.host"));
    Assert.assertEquals("localhost",
        subject.get("test.scm.client.address"));
    Assert.assertTrue(
        subject.getBoolean("test.scm.client.enabled", false));
    Assert.assertEquals(9878,
        subject.getInt("test.scm.client.port", 123));
    Assert.assertEquals(TimeUnit.MINUTES.toSeconds(30),
        subject.getTimeDuration("test.scm.client.wait", 555, TimeUnit.SECONDS));
  }

  @Test
  public void setConfigFromObjectWithObjectDefaults() {
    // GIVEN
    SimpleConfiguration object = new SimpleConfiguration();
    OzoneConfiguration subject = new OzoneConfiguration();

    // WHEN
    subject.setFromObject(object);

    // THEN
    Assert.assertEquals("0.0.0.0",
        subject.get("test.scm.client.bind.host"));
    Assert.assertEquals("localhost",
        subject.get("test.scm.client.address"));
    Assert.assertFalse(
        subject.getBoolean("test.scm.client.enabled", false));
    Assert.assertEquals(0,
        subject.getInt("test.scm.client.port", 123));
    Assert.assertEquals(0,
        subject.getTimeDuration("test.scm.client.wait", 555, TimeUnit.SECONDS));
  }

  @Test(expected = NumberFormatException.class)
  public void postConstructValidation() {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setInt("test.scm.client.port", -3);

    ozoneConfiguration.getObject(SimpleConfiguration.class);
  }

  private void appendProperty(BufferedWriter out, String name, String val)
      throws IOException {
    this.appendProperty(out, name, val, false);
  }

  private void appendProperty(BufferedWriter out, String name, String val,
                              boolean isFinal) throws IOException {
    out.write("<property>");
    out.write("<name>");
    out.write(name);
    out.write("</name>");
    out.write("<value>");
    out.write(val);
    out.write("</value>");
    if (isFinal) {
      out.write("<final>true</final>");
    }
    out.write("</property>\n");
  }

  private void appendPropertyByTag(BufferedWriter out, String name, String val,
                                   String tags) throws IOException {
    this.appendPropertyByTag(out, name, val, false, tags);
  }

  private void appendPropertyByTag(BufferedWriter out, String name, String val,
                                   boolean isFinal,
                                   String tag) throws IOException {
    out.write("<property>");
    out.write("<name>");
    out.write(name);
    out.write("</name>");
    out.write("<value>");
    out.write(val);
    out.write("</value>");
    if (isFinal) {
      out.write("<final>true</final>");
    }
    out.write("<tag>");
    out.write(tag);
    out.write("</tag>");
    out.write("</property>\n");
  }
}
