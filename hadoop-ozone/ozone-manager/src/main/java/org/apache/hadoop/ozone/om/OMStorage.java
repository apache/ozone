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

package org.apache.hadoop.ozone.om;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;

/**
 * Ozone Manager VERSION file representation.
 * On top of what is defined in the base Storage class, this class adds
 * functionality to hold Ozone Manager related data in its VERSION file.
 * The additional values stored:
 * - Ozone Manager ID - a UUID that identifies this Ozone Manager.
 *                      The value can not be changed once initialized, and
 *                      it is initialized automatically in this class.
 *                      The value itself is not used anymore, it is part of the
 *                      {@link org.apache.hadoop.hdds.protocol.proto.HddsProtos
 *                      .OzoneManagerDetailsProto} hence not removed yet.
 * - Ozone Manager Node Id - the node id defined for this Ozone manager in the
 *                           configuration. The value can not be changed after
 *                           it was set.
 * - Ozone Manager Certificate Serial Id - the serial id of the Ozone Manager's
 *                                         SSL certificate if one exists.
 */
public class OMStorage extends Storage {

  public static final String TESTING_INIT_LAYOUT_VERSION_KEY = "testing.ozone.om.init.layout.version";

  static final String ERROR_OM_IS_ALREADY_INITIALIZED =
      "OM is already initialized.";
  static final String ERROR_UNEXPECTED_OM_NODE_ID_TEMPLATE =
      "Configured OM NodeId: %s does not match the existing nodeId in the " +
      "VERSION file: %s.%n" + "One of the possible reasons may be, that " +
      "previously an other OM was running on this host and some metadata was " +
      "left in the same directory that is configured for this OM instance as " +
      "metadata directory, or someone changed the associated OM id in " +
      "the configuration.%n" + "Please carefully assess the configuration" +
      " and the VERSION file in the metadata directory, and either change" +
      " the configuration back, or remove the metadata directory, and " +
      "re-initialize this Ozone Manager. (Changing the VERSION file is not" +
      " a good approach, as the OM id is an integral part of the Raft " +
      "metadata also.%n" + "Note that removing the metadata from the " +
      "majority of Ozone Managers at once can lead to loosing the filesystem" +
      " metadata, so take extra care if you do remove the metadata and that" +
      " data is not a leftover from previous installations.%n";
  static final String ERROR_STORAGE_NOT_INITIALIZED =
      "OM Storage is not initialized yet.";

  static final String STORAGE_DIR = "om";
  static final String OM_ID = "omUuid";
  static final String OM_CERT_SERIAL_ID = "omCertSerialId";
  static final String OM_NODE_ID = "nodeId";

  /**
   * Construct the OMStorage instance based on the configuration.
   * The parent directory used by the storage is defined by the
   * {@link OMConfigKeys#OZONE_OM_DB_DIRS} property, if that is not set the
   * {@link org.apache.hadoop.hdds.HddsConfigKeys#OZONE_METADATA_DIRS} property
   * value is used as a fallback. If none of these are defined in the
   * configuration an IllegalArgumentException is being thrown.
   *
   * @param conf an OzoneConfiguration instance containing the properties that
   *             can define the path where Ozone Manager stores its metadata
   *
   * @throws IOException if any directories are inaccessible.
   * @throws IllegalArgumentException if the configuration does not specify the
   *                               path where the metadata should be stored
   */
  public OMStorage(OzoneConfiguration conf) throws IOException {
    super(NodeType.OM, getOmDbDir(conf), STORAGE_DIR,
        getInitLayoutVersion(conf, TESTING_INIT_LAYOUT_VERSION_KEY,
            OMLayoutVersionManager::maxLayoutVersion));
  }

  /**
   * Sets the certificate serial id to be stored in the VERSION file
   * representation.
   * Note that, to change the VERSION file itself,
   * {@link #persistCurrentState()} has to be called after this method.
   *
   * @param certSerialId the new certificate serial id to set
   *
   * @throws IOException if the current VERSION file is not readable
   */
  public void setOmCertSerialId(String certSerialId) throws IOException {
    getStorageInfo().setProperty(OM_CERT_SERIAL_ID, certSerialId);
  }

  /**
   * Removes the certificate serial id from the VERSION file representation.
   * Note that, to change the VERSION file itself,
   * {@link #persistCurrentState()} has to be called after this method.
   */
  public void unsetOmCertSerialId() {
    getStorageInfo().unsetProperty(OM_CERT_SERIAL_ID);
  }

  /**
   * Set's the Ozone Manager ID to be stored in the VERSION file representation.
   * Note that, to change the VERSION file itself,
   * {@link #persistCurrentState()} has to be called after this method.
   *
   * @param omId the UUID that identifies this Ozone Manager as a String
   *
   * @throws IOException if the Storage representation is already initialized,
   *                     as this property can not be changed once it has been
   *                     set and stored
   */
  public void setOmId(String omId) throws IOException {
    if (getState() == StorageState.INITIALIZED) {
      throw new IOException(ERROR_OM_IS_ALREADY_INITIALIZED);
    } else {
      getStorageInfo().setProperty(OM_ID, omId);
    }
  }

  /**
   * Set's the Ozone Manager Node ID.
   * This value should be set based on the configuration and should not be
   * changed later on neither in the configuration nor in the VERSION file
   * to ensure consistency within the Ozone Manager HA peers.
   *
   * Note that, to change the VERSION file itself,
   * {@link #persistCurrentState()} has to be called after this method.
   *
   * @param nodeId the UUID that identifies this Ozone Manager as a String
   *
   * @throws IOException if the Storage representation is already initialized,
   *                     as this property can not be changed once it has been
   *                     set and stored.
   */
  public void setOmNodeId(String nodeId)
      throws IOException {
    if (getState() == StorageState.INITIALIZED) {
      throw new IOException(ERROR_OM_IS_ALREADY_INITIALIZED);
    } else {
      getStorageInfo().setProperty(OM_NODE_ID, nodeId);
    }
  }

  /**
   * Validates if the provided value is the one saved in the VERSION file.
   * This method provides a convenience to check if the configured value and
   * the one that was stored in the VERSION file are matching.
   *
   * As a VERSION file that was created by an older version of OM might not
   * contain the value, if the VERSION file does not have this property, the
   * method persists the provided expectedNodeId into the VERSION file
   * and skips the validation.
   *
   * @param expectedNodeId the nodeId read from configuration, that has to be
   *                       matched against what we have saved in the VERSION
   *                       file
   *
   * @throws IOException - if the VERSION file is not present at the time of the
   *                        call
   *                     - if the VERSION file contains a different value than
   *                        the expectedNodeId provided
   *                     - if reading/writing the VERSION file fails
   */
  /* Note that we have other options as well to handle this case, but at this
   * time this seemed to be a good tradeoff.
   * Other options:
   *   1. Use the Upgrade framework and bump Layout version.
   *     Excessive work, and the addition, with that the verification happens
   *     too late, after a bunch of things has been initialized, and there
   *     might be possible problems from the late validation.
   *   2. Write the value during omInit only as with anything added earlier
   *     Seems to be error-prone, as without re-initializing the OM, the value
   *     will not get to the VERSION file, and validation will fail.
   * This approach has the potential to scatter OzoneManager constructor call,
   *   so if anything like this one is to be implemented, try to figure out
   *   a better way, or switch to another approach if feasible.
   */
  public void validateOrPersistOmNodeId(String expectedNodeId)
      throws IOException {
    if (getState() != StorageState.INITIALIZED) {
      throw new IOException(ERROR_STORAGE_NOT_INITIALIZED);
    }
    String ourValue = getOmNodeId();
    if (ourValue != null && !ourValue.equals(expectedNodeId)) {
      String msg = String.format(
          ERROR_UNEXPECTED_OM_NODE_ID_TEMPLATE, expectedNodeId, ourValue);
      throw new IOException(msg);
    } else {
      getStorageInfo().setProperty(OM_NODE_ID, expectedNodeId);
      persistCurrentState();
    }
  }

  /**
   * Retrieves the OM ID from the version file.
   *
   * @return the stored OM ID
   */
  public String getOmId() {
    return getStorageInfo().getProperty(OM_ID);
  }

  /**
   * Retrieves the OM NodeId from the version file.
   *
   * @return the stored OM Node ID
   */
  public String getOmNodeId() {
    return getStorageInfo().getProperty(OM_NODE_ID);
  }

  /**
   * Retrieves the serial id of certificate issued by SCM.
   *
   * @return the stored Certificate Serial ID
   */
  public String getOmCertSerialId() {
    return getStorageInfo().getProperty(OM_CERT_SERIAL_ID);
  }

  @Override
  protected Properties getNodeProperties() {
    String omId = getOmId();
    if (omId == null) {
      omId = UUID.randomUUID().toString();
    }
    Properties omProperties = new Properties();
    omProperties.setProperty(OM_ID, omId);
    String nodeId = getOmNodeId();
    if (nodeId != null) {
      omProperties.setProperty(OM_NODE_ID, nodeId);
    }

    if (getOmCertSerialId() != null) {
      omProperties.setProperty(OM_CERT_SERIAL_ID, getOmCertSerialId());
    }
    return omProperties;
  }

  /**
   * From the provided configuration gets the directory that Ozone Manager
   * should use to store its metadata.
   * The value of {@link OMConfigKeys#OZONE_OM_DB_DIRS} property is returned
   * as the primary value, if that is not set, the method falls back to the
   * {@link org.apache.hadoop.hdds.HddsConfigKeys#OZONE_METADATA_DIRS} property.
   * If none of these are defined, an IllegalArgumentException is being thrown.
   *
   * @param conf - the configuration to get the properties from
   *
   * @return The metadata directory path, that should be used by OM, after
   *         creating all the necessary directories
   *
   * @throws IllegalArgumentException if the metadata directory can not be
   *                                  determined from the configuration
   */
  public static File getOmDbDir(ConfigurationSource conf) {
    return ServerUtils.getDBPath(conf, OMConfigKeys.OZONE_OM_DB_DIRS);
  }
}
