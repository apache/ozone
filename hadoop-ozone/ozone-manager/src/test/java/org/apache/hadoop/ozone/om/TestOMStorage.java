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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import static org.apache.hadoop.ozone.common.Storage.StorageState.INITIALIZED;
import static org.apache.hadoop.ozone.om.OMStorage.ERROR_OM_IS_ALREADY_INITIALIZED;
import static org.apache.hadoop.ozone.om.OMStorage.ERROR_UNEXPECTED_OM_NODE_ID_TEMPLATE;
import static org.apache.hadoop.ozone.om.OMStorage.OM_CERT_SERIAL_ID;
import static org.apache.hadoop.ozone.om.OMStorage.OM_ID;
import static org.apache.hadoop.ozone.om.OMStorage.OM_NODE_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

/**
 * Testing OMStorage class.
 * Assumptions tested:
 *   1. certificate serial ID can be set and unset anytime.
 *   2. OmId the UUID of the Ozone Manager can be set only when the OMStorage
 *       is not initialized already. Once initialized, setting OmId throws
 *       IOException
 *   3. OmNodeId:
 *     3.1. can be set when the storage is not initialized, once initialize,
 *         setting OmNodeId throws IOException
 *     3.2. verifying the OmNodeId is possible once the storage is initialized,
 *         until it is not initialized, verification throws IOException
 *     3.3. verifying the OmNodeId does not do anything if the provided value is
 *         equal to the stored value, throws an IOException otherwise
 *   4. Configuration parsing:
 *     4.1. getOmDbDir returns the configured
 *         {@link OMConfigKeys#OZONE_OM_DB_DIRS} value
 *     4.2. getOmDbDir falls back to {@link HddsConfigKeys#OZONE_METADATA_DIRS}
 *         when {@link OMConfigKeys#OZONE_OM_DB_DIRS} is not set
 *     4.3. getOmDbDir throws exception if none of the above properties are set
 *   5. the protected getNodeProperties method properly returns all the keys
 *       that are set properly in the OMStorage object.
 */
public class TestOMStorage {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final String OM_ID_STR = new UUID(1L, 1L).toString();

  @Test
  public void testGetOmDbDir() throws Exception {
    final File testDir = tmpFolder.newFolder();
    final File dbDir = new File(testDir, "omDbDir");
    final File metaDir = new File(testDir, "metaDir");
    OzoneConfiguration conf = confWithHDDSMetaAndOMDBDir(metaDir, dbDir);

    assertThat(dbDir, equalTo(OMStorage.getOmDbDir(conf)));
    assertThat(dbDir.exists(), is(true));
    assertThat(metaDir.exists(), is(false));
  }

  @Test
  public void testGetOmDbDirWithFallback() throws Exception {
    File metaDir = tmpFolder.newFolder();
    OzoneConfiguration conf = confWithHDDSMetadataDir(metaDir);

    assertThat(metaDir, equalTo(OMStorage.getOmDbDir(conf)));
    assertThat(metaDir.exists(), is(true));
  }

  @Test
  public void testNoOmDbDirConfigured() {
    thrown.expect(IllegalArgumentException.class);
    OMStorage.getOmDbDir(new OzoneConfiguration());
  }

  @Test
  public void testSetOmIdOnNotInitializedStorage() throws Exception {
    OMStorage storage = new OMStorage(configWithOMDBDir());
    assertThat(storage.getState(), is(not(INITIALIZED)));

    String omId = "omId";
    try {
      storage.setOmId(omId);
    } catch (IOException e) {
      fail("Can not set OmId on a Storage that is not initialized.");
    }
    assertThat(storage.getOmId(), is(omId));
    assertGetNodeProperties(storage, omId);
  }

  @Test
  public void testSetOmIdOnInitializedStorage() throws Exception {
    OzoneConfiguration conf = configWithOMDBDir();
    setupAPersistedVersionFile(conf);
    thrown.expect(IOException.class);
    thrown.expectMessage(ERROR_OM_IS_ALREADY_INITIALIZED);

    OMStorage storage = new OMStorage(conf);
    storage.setOmId("omId");
  }

  @Test
  public void testCertSerialIdOperations() throws Exception {
    OzoneConfiguration conf = configWithOMDBDir();
    OMStorage storage = new OMStorage(conf);

    assertThat(storage.getState(), is(not(INITIALIZED)));
    assertCertOps(storage);
    storage.initialize();
    storage.persistCurrentState();

    storage = new OMStorage(conf);
    assertThat(storage.getState(), is(INITIALIZED));
    assertCertOps(storage);
  }

  @Test
  public void testSetOmNodeIdOnNotInitializedStorage() throws Exception {
    OMStorage storage = new OMStorage(configWithOMDBDir());
    assertThat(storage.getState(), is(not(INITIALIZED)));

    String nodeId = "nodeId";
    try {
      storage.setOmNodeId(nodeId);
    } catch (IOException e) {
      fail("Can not set OmNodeId on a Storage that is not initialized.");
    }
    assertThat(storage.getOmNodeId(), is(nodeId));
    assertGetNodeProperties(storage, null, nodeId);
  }

  @Test
  public void testSetOMNodeIdOnInitializedStorageWithoutNodeID()
      throws Exception {
    OzoneConfiguration conf = configWithOMDBDir();
    setupAPersistedVersionFile(conf);
    thrown.expect(IOException.class);
    thrown.expectMessage(ERROR_OM_IS_ALREADY_INITIALIZED);

    OMStorage storage = new OMStorage(conf);
    storage.setOmNodeId("nodeId");
  }

  @Test
  public void testSetOMNodeIdOnInitializedStorageWithNodeID() throws Exception {
    OzoneConfiguration conf = configWithOMDBDir();
    setupAPersistedVersionFileWithNodeId(conf, "nodeId");
    thrown.expect(IOException.class);
    thrown.expectMessage(ERROR_OM_IS_ALREADY_INITIALIZED);

    OMStorage storage = new OMStorage(conf);
    storage.setOmNodeId("nodeId");
  }

  @Test
  public void testValidateOrPersistOmNodeIdPersistsNewlySetValue()
      throws Exception {
    String nodeId = "nodeId";
    OzoneConfiguration conf = configWithOMDBDir();
    setupAPersistedVersionFile(conf);

    OMStorage storage = new OMStorage(conf);
    assertThat(storage.getState(), is(INITIALIZED));
    assertThat(storage.getOmNodeId(), is(nullValue()));

    storage.validateOrPersistOmNodeId(nodeId);
    assertThat(storage.getOmNodeId(), is(nodeId));
    assertGetNodeProperties(storage, OM_ID_STR, nodeId);

    storage = new OMStorage(conf);
    assertThat(storage.getOmNodeId(), is(nodeId));
    assertGetNodeProperties(storage, OM_ID_STR, nodeId);
  }

  @Test
  public void testValidateOrPersistOmNodeIdDoesRunWithSameNodeIdAsInFile()
      throws Exception {
    String nodeId = "nodeId";
    OzoneConfiguration conf = configWithOMDBDir();
    setupAPersistedVersionFileWithNodeId(conf, nodeId);

    OMStorage storage = new OMStorage(conf);
    assertThat(storage.getState(), is(INITIALIZED));
    assertThat(storage.getOmNodeId(), is(nodeId));
    assertGetNodeProperties(storage, OM_ID_STR, nodeId);

    storage.validateOrPersistOmNodeId(nodeId);

    assertThat(storage.getOmNodeId(), is(nodeId));
    assertGetNodeProperties(storage, OM_ID_STR, nodeId);
  }

  @Test
  public void testValidateOrPersistOmNodeIdThrowsWithDifferentNodeIdAsInFile()
      throws Exception {
    String nodeId = "nodeId";
    String newId = "newId";
    OzoneConfiguration conf = configWithOMDBDir();
    setupAPersistedVersionFileWithNodeId(conf, nodeId);

    OMStorage storage = new OMStorage(conf);
    assertThat(storage.getState(), is(INITIALIZED));
    assertThat(storage.getOmNodeId(), is(nodeId));

    thrown.expect(IOException.class);
    String expectedMsg =
        String.format(ERROR_UNEXPECTED_OM_NODE_ID_TEMPLATE, newId, nodeId);
    thrown.expectMessage(expectedMsg);

    storage.validateOrPersistOmNodeId(newId);
  }

  private void assertCertOps(OMStorage storage) throws IOException {
    String certSerialId = "12345";
    String certSerialId2 = "54321";
    storage.setOmCertSerialId(certSerialId);
    assertThat(storage.getOmCertSerialId(), is(certSerialId));
    assertGetNodeProperties(storage, null, null, certSerialId);

    storage.setOmCertSerialId(certSerialId2);
    assertThat(storage.getOmCertSerialId(), is(certSerialId2));
    assertGetNodeProperties(storage, null, null, certSerialId2);

    storage.unsetOmCertSerialId();
    assertThat(storage.getOmCertSerialId(), is(nullValue()));
    assertGetNodeProperties(storage, null, null, null);
  }

  private void assertGetNodeProperties(OMStorage storage, String... values) {
    Properties p = storage.getNodeProperties();
    Map<String, String> e = toExpectedPropertyMapping(values);

    if (e.get(OM_ID) != null) {
      assertThat(p.getProperty(OM_ID), is(e.get(OM_ID)));
    }
    if (e.get(OM_NODE_ID) != null) {
      assertThat(p.get(OM_NODE_ID), is(e.get(OM_NODE_ID)));
    }
    if (e.get(OM_CERT_SERIAL_ID) != null) {
      assertThat(p.get(OM_CERT_SERIAL_ID), is(e.get(OM_CERT_SERIAL_ID)));
    }
  }

  private Map<String, String> toExpectedPropertyMapping(String... values) {
    Map<String, String> ret = new HashMap<>();
    String[] propNames = new String[]{OM_ID, OM_NODE_ID, OM_CERT_SERIAL_ID};
    for (int i = 0; i < values.length; i++) {
      ret.put(propNames[i], values[i]);
    }
    return ret;
  }

  private void setupAPersistedVersionFile(OzoneConfiguration conf)
      throws IOException {
    setupAPersistedVersionFileWithNodeId(conf, null);
  }

  private void setupAPersistedVersionFileWithNodeId(
      OzoneConfiguration conf, String nodeId) throws IOException {
    OMStorage storage = new OMStorage(conf);
    storage.setClusterId("clusterId");
    storage.setLayoutVersion(OMLayoutVersionManager.maxLayoutVersion());
    storage.setOmId(OM_ID_STR);
    if (nodeId != null) {
      storage.setOmNodeId(nodeId);
    }
    storage.initialize();
    storage.persistCurrentState();
  }

  private OzoneConfiguration configWithOMDBDir() throws IOException {
    File dir = tmpFolder.newFolder();
    return configWithOMDBDir(dir);
  }

  private OzoneConfiguration confWithHDDSMetaAndOMDBDir(
      File metaDir, File dbDir) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getAbsolutePath());
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbDir.getAbsolutePath());
    return conf;
  }

  private OzoneConfiguration confWithHDDSMetadataDir(File dir) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.getAbsolutePath());
    return conf;
  }

  private OzoneConfiguration configWithOMDBDir(File dir) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dir.getAbsolutePath());
    return conf;
  }

}
