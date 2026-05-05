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

package org.apache.hadoop.hdds.conf;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for OzoneConfiguration.
 */
public class TestOzoneConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestOzoneConfiguration.class);

  private OzoneConfiguration conf;

  @BeforeEach
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
  public void testGetAllPropertiesByTags(@TempDir File tempDir)
      throws Exception {
    File coreDefault = new File(tempDir, "core-default-test.xml");
    File coreSite = new File(tempDir, "core-site-test.xml");
    OutputStream coreDefaultStream = Files.newOutputStream(coreDefault.toPath());
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
      assertEquals("XYZ", conf.getAllPropertiesByTag("MYCUSTOMTAG")
          .getProperty("dfs.random.key"));
    }

    OutputStream coreSiteStream = Files.newOutputStream(coreSite.toPath());
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
    assertEquals("3", conf.getAllPropertiesByTag("HDFS")
        .getProperty("dfs.replication"));
    assertEquals("ABC", conf.getAllPropertiesByTag("MYCUSTOMTAG")
        .getProperty("dfs.random.key"));
    assertEquals("true", conf.getAllPropertiesByTag("YARN")
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
    ozoneConfig.setTimeDuration("test.scm.client.duration",
        3, TimeUnit.SECONDS);
    ozoneConfig.set("test.scm.client.class", Integer.class.getName());
    ozoneConfig.setDouble("test.scm.client.threshold", 10.5);

    SimpleConfiguration configuration =
        ozoneConfig.getObject(SimpleConfiguration.class);

    assertEquals("host", configuration.getBindHost());
    assertEquals("address", configuration.getClientAddress());
    assertEquals(5555, configuration.getPort());
    assertEquals(600, configuration.getWaitTime());
    assertSame(Integer.class, configuration.getMyClass());
    assertEquals(10.5, configuration.getThreshold());
    assertEquals(Duration.ofSeconds(3), configuration.getDuration());
  }

  @Test
  public void testRestrictedComplianceModeWithOzoneConf() {
    Configuration config = new Configuration();
    config.set("ozone.security.crypto.compliance.mode", "restricted");
    OzoneConfiguration ozoneConfig = new OzoneConfiguration(config);

    // Set it to an allowed config value
    ozoneConfig.set("hdds.x509.signature.algorithm", "SHA512withDCA");
    ozoneConfig.set("hdds.x509.signature.algorithm.restricted.whitelist", "SHA512withRSA,SHA512withDCA");

    assertEquals("restricted", ozoneConfig.get("ozone.security.crypto.compliance.mode"));
    assertEquals("SHA512withRSA,SHA512withDCA", ozoneConfig.get("hdds.x509.signature.algorithm.restricted.whitelist"));
    assertEquals("SHA512withDCA", ozoneConfig.get("hdds.x509.signature.algorithm"));

    // Set it to a disallowed config value
    ozoneConfig.set("hdds.x509.signature.algorithm", "SHA256withRSA");

    assertThrows(ConfigurationException.class, () -> ozoneConfig.get("hdds.x509.signature.algorithm"));

    // Check it with a Hadoop Configuration
    Configuration hadoopConfig =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(ozoneConfig);
    assertThrows(ConfigurationException.class, () -> hadoopConfig.get("hdds.x509.signature.algorithm"));
  }

  @Test
  public void testRestrictedComplianceModeWithLegacyHadoopConf() {
    Configuration config = new Configuration();
    config.addResource("ozone-default.xml");
    config.set("ozone.security.crypto.compliance.mode", "restricted");
    LegacyHadoopConfigurationSource legacyHadoopConf = new LegacyHadoopConfigurationSource(config);

    // Set it to an allowed config value
    legacyHadoopConf.set("hdds.x509.signature.algorithm", "SHA512withDCA");
    legacyHadoopConf.set("hdds.x509.signature.algorithm.restricted.whitelist", "SHA512withRSA,SHA512withDCA");

    assertEquals("restricted", legacyHadoopConf.get("ozone.security.crypto.compliance.mode"));
    assertEquals("SHA512withRSA,SHA512withDCA",
        legacyHadoopConf.get("hdds.x509.signature.algorithm.restricted.whitelist"));
    assertEquals("SHA512withDCA", legacyHadoopConf.get("hdds.x509.signature.algorithm"));

    // Set it to a disallowed config value
    legacyHadoopConf.set("hdds.x509.signature.algorithm", "SHA256withRSA");

    assertThrows(ConfigurationException.class, () -> legacyHadoopConf.get("hdds.x509.signature.algorithm"));

    // Check it with a Hadoop Configuration
    Configuration legacyConf = LegacyHadoopConfigurationSource.asHadoopConfiguration(legacyHadoopConf);
    assertThrows(ConfigurationException.class, () -> legacyConf.get("hdds.x509.signature.algorithm"));
  }

  @Test
  public void testUnrestrictedComplianceMode() {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set("hdds.x509.signature.algorithm", "SHA256");
    ozoneConfig.set("hdds.x509.signature.algorithm.unrestricted.whitelist", "SHA512withRSA");

    assertEquals(ozoneConfig.get("hdds.x509.signature.algorithm"), "SHA256");
    assertEquals(ozoneConfig.get("ozone.security.crypto.compliance.mode"), "unrestricted");
  }

  @Test
  public void getConfigurationObjectWithDefault() {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();

    SimpleConfiguration configuration =
        ozoneConfiguration.getObject(SimpleConfiguration.class);

    assertEquals(9878, configuration.getPort());
    assertSame(Object.class, configuration.getMyClass());
    assertEquals(10, configuration.getThreshold());
    assertEquals(Duration.ofHours(1), configuration.getDuration());
  }

  @Test
  public void setConfigFromObject() {
    // GIVEN
    SimpleConfiguration object = new SimpleConfiguration();
    object.setBindHost("host");
    object.setClientAddress("address");
    object.setPort(5555);
    object.setWaitTime(600);
    object.setMyClass(this.getClass());
    object.setThreshold(10.5);
    object.setDuration(Duration.ofMillis(100));

    OzoneConfiguration subject = new OzoneConfiguration();

    // WHEN
    subject.setFromObject(object);

    // THEN
    assertEquals(object.getBindHost(), subject.get("test.scm.client.bind.host"));
    assertEquals(object.getClientAddress(), subject.get("test.scm.client.address"));
    assertEquals(object.getPort(), subject.getInt("test.scm.client.port", 0));
    assertEquals(TimeUnit.SECONDS.toMinutes(object.getWaitTime()),
        subject.getTimeDuration("test.scm.client.wait", 0, TimeUnit.MINUTES));
    assertSame(this.getClass(),
        subject.getClass("test.scm.client.class", null));
    assertEquals(object.getThreshold(),
        subject.getDouble("test.scm.client.threshold", 20.5));
    assertEquals(object.getDuration().toMillis(),
        subject.getTimeDuration("test.scm.client.duration", 0,
            TimeUnit.MILLISECONDS));
  }

  @Test
  public void setConfigFromObjectWithConfigDefaults() {
    // GIVEN
    OzoneConfiguration subject = new OzoneConfiguration();
    SimpleConfiguration object = subject.getObject(SimpleConfiguration.class);

    // WHEN
    subject.setFromObject(object);

    // THEN
    assertEquals("0.0.0.0", subject.get("test.scm.client.bind.host"));
    assertEquals("localhost", subject.get("test.scm.client.address"));
    assertEquals(9878, subject.getInt("test.scm.client.port", 123));
    assertEquals(TimeUnit.MINUTES.toSeconds(30),
        subject.getTimeDuration("test.scm.client.wait", 555, TimeUnit.SECONDS));
    assertEquals(10, subject.getDouble("test.scm.client.threshold", 20.5));
  }

  @Test
  public void testInstantiationWithInputConfiguration(@TempDir File tempDir)
      throws IOException {
    String key = "hdds.scm.init.default.layout.version";
    String val = "Test1";
    Configuration configuration = new Configuration(true);

    File ozoneSite = new File(tempDir, "ozone-site.xml");
    OutputStream ozoneSiteStream = Files.newOutputStream(ozoneSite.toPath());
    try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
        ozoneSiteStream, StandardCharsets.UTF_8))) {
      startConfig(out);
      appendProperty(out, key, val);
      endConfig(out);
    }
    configuration
        .addResource(new URL("file:///" + ozoneSite.getAbsolutePath()));

    OzoneConfiguration ozoneConfiguration =
        new OzoneConfiguration(configuration);
    // ozoneConfig value matches input config value for the corresponding key
    assertEquals(val, ozoneConfiguration.get(key));
    assertEquals(val, configuration.get(key));

    assertNotEquals(val, new OzoneConfiguration().get(key));
  }

  @Test
  public void setConfigFromObjectWithObjectDefaults() {
    // GIVEN
    SimpleConfiguration object = new SimpleConfiguration();
    OzoneConfiguration subject = new OzoneConfiguration();

    // WHEN
    subject.setFromObject(object);

    // THEN
    assertEquals("0.0.0.0", subject.get("test.scm.client.bind.host"));
    assertEquals("localhost", subject.get("test.scm.client.address"));
    assertFalse(subject.getBoolean("test.scm.client.enabled", false));
    assertEquals(0, subject.getInt("test.scm.client.port", 123));
    assertEquals(0, subject.getTimeDuration("test.scm.client.wait", 555, TimeUnit.SECONDS));
    assertEquals(0, subject.getDouble("test.scm.client.threshold", 20.5));
  }

  private static Stream<Arguments> getIntBackwardCompatibilityScenarios() {
    return Stream.of(
        Arguments.of(OZONE_SCM_CLIENT_HANDLER_COUNT_KEY, 10, true,
            OZONE_SCM_HANDLER_COUNT_KEY, 10, OZONE_SCM_HANDLER_COUNT_DEFAULT),
        Arguments.of(OZONE_SCM_BLOCK_HANDLER_COUNT_KEY, -1, false,
            OZONE_SCM_HANDLER_COUNT_KEY, OZONE_SCM_HANDLER_COUNT_DEFAULT,
                OZONE_SCM_HANDLER_COUNT_DEFAULT),
        Arguments.of(OZONE_SCM_DATANODE_HANDLER_COUNT_KEY, 105, true,
            OZONE_SCM_HANDLER_COUNT_KEY, 105, OZONE_SCM_HANDLER_COUNT_DEFAULT)
    );
  }

  @ParameterizedTest
  @MethodSource("getIntBackwardCompatibilityScenarios")
  public void testGetIntBackwardCompatibility(String name, int newVal,
      boolean isGen, String fallbackName, int targetVal, int defaultVal) {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    if (isGen) {
      ozoneConfig.setInt(name, newVal);
    }
    int value = ozoneConfig.getInt(name, fallbackName, defaultVal, LOG::info);
    assertEquals(value, targetVal);
  }

  @Test
  public void postConstructValidation() {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setInt("test.scm.client.port", -3);

    assertThrows(NumberFormatException.class,
        () -> ozoneConfiguration.getObject(SimpleConfiguration.class));
  }

  @ParameterizedTest
  @EnumSource
  void tagIsRecognized(ConfigTag tag) {
    OzoneConfiguration subject = new OzoneConfiguration();
    assertTrue(subject.isPropertyTag(tag.name()),
        () -> tag + " should be recognized as config tag");
  }

  @Test
  void unknownTag() {
    OzoneConfiguration subject = new OzoneConfiguration();
    assertFalse(subject.isPropertyTag("not-a-tag"));
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
