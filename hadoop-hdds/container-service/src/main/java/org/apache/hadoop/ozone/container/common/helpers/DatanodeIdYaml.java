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

package org.apache.hadoop.ozone.container.common.helpers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.YamlUtils;
import org.apache.hadoop.hdds.upgrade.BelongsToHDDSLayoutVersion;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * Class for creating datanode.id file in yaml format.
 */
public final class DatanodeIdYaml {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeIdYaml.class);

  private DatanodeIdYaml() {
    // static helper methods only, no state.
  }

  /**
   * Creates a yaml file using DatanodeDetails. This method expects the path
   * validation to be performed by the caller.
   *
   * @param datanodeDetails {@link DatanodeDetails}
   * @param path            Path to datnode.id file
   */
  public static void createDatanodeIdFile(DatanodeDetails datanodeDetails,
                                          File path,
                                          ConfigurationSource conf)
      throws IOException {
    DumperOptions options = new DumperOptions();
    options.setPrettyFlow(true);
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);
    Yaml yaml = new Yaml(options);

    final DatanodeDetailsYaml data = getDatanodeDetailsYaml(datanodeDetails, conf);
    YamlUtils.dump(yaml, data, path, LOG);
  }

  /**
   * Read datanode.id from file.
   */
  public static DatanodeDetails readDatanodeIdFile(File path)
      throws IOException {
    DatanodeDetails datanodeDetails;
    try (InputStream inputFileStream = Files.newInputStream(path.toPath())) {
      DatanodeDetailsYaml datanodeDetailsYaml;
      try {
        datanodeDetailsYaml =
            YamlUtils.loadAs(inputFileStream, DatanodeDetailsYaml.class);
      } catch (Exception e) {
        throw new IOException("Unable to parse yaml file.", e);
      }

      DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
      builder.setUuid(UUID.fromString(datanodeDetailsYaml.getUuid()))
          .setIpAddress(datanodeDetailsYaml.getIpAddress())
          .setHostName(datanodeDetailsYaml.getHostName())
          .setCertSerialId(datanodeDetailsYaml.getCertSerialId());
      if (datanodeDetailsYaml.getPersistedOpState() != null) {
        builder.setPersistedOpState(HddsProtos.NodeOperationalState.valueOf(
            datanodeDetailsYaml.getPersistedOpState()));
      }
      builder.setPersistedOpStateExpiry(
          datanodeDetailsYaml.getPersistedOpStateExpiryEpochSec());

      if (!MapUtils.isEmpty(datanodeDetailsYaml.getPortDetails())) {
        for (Map.Entry<String, Integer> portEntry :
            datanodeDetailsYaml.getPortDetails().entrySet()) {
          builder.addPort(DatanodeDetails.newPort(
              DatanodeDetails.Port.Name.valueOf(portEntry.getKey()),
              portEntry.getValue()));
        }
      }

      builder.setInitialVersion(datanodeDetailsYaml.getInitialVersion())
          .setCurrentVersion(datanodeDetailsYaml.getCurrentVersion());

      datanodeDetails = builder.build();
    }

    return datanodeDetails;
  }

  /**
   * Datanode details bean to be written to the yaml file.
   */
  public static class DatanodeDetailsYaml {
    private String uuid;
    private String ipAddress;
    private String hostName;
    private String certSerialId;
    private String persistedOpState;
    private long persistedOpStateExpiryEpochSec = 0;
    private Map<String, Integer> portDetails;
    private int initialVersion;
    private int currentVersion;

    public DatanodeDetailsYaml() {
      // Needed for snake-yaml introspection.
    }

    @SuppressWarnings({"parameternumber", "java:S107"}) // required for yaml
    private DatanodeDetailsYaml(String uuid, String ipAddress,
        String hostName, String certSerialId,
        String persistedOpState, long persistedOpStateExpiryEpochSec,
        Map<String, Integer> portDetails,
        int initialVersion, int currentVersion) {
      this.uuid = uuid;
      this.ipAddress = ipAddress;
      this.hostName = hostName;
      this.certSerialId = certSerialId;
      this.persistedOpState = persistedOpState;
      this.persistedOpStateExpiryEpochSec = persistedOpStateExpiryEpochSec;
      this.portDetails = portDetails;
      this.initialVersion = initialVersion;
      this.currentVersion = currentVersion;
    }

    public String getUuid() {
      return uuid;
    }

    public String getIpAddress() {
      return ipAddress;
    }

    public String getHostName() {
      return hostName;
    }

    public String getCertSerialId() {
      return certSerialId;
    }

    public String getPersistedOpState() {
      return persistedOpState;
    }

    public long getPersistedOpStateExpiryEpochSec() {
      return persistedOpStateExpiryEpochSec;
    }

    public Map<String, Integer> getPortDetails() {
      return portDetails;
    }

    public void setUuid(String uuid) {
      this.uuid = uuid;
    }

    public void setIpAddress(String ipAddress) {
      this.ipAddress = ipAddress;
    }

    public void setHostName(String hostName) {
      this.hostName = hostName;
    }

    public void setCertSerialId(String certSerialId) {
      this.certSerialId = certSerialId;
    }

    public void setPersistedOpState(String persistedOpState) {
      this.persistedOpState = persistedOpState;
    }

    public void setPersistedOpStateExpiryEpochSec(long opStateExpiryEpochSec) {
      this.persistedOpStateExpiryEpochSec = opStateExpiryEpochSec;
    }

    public void setPortDetails(Map<String, Integer> portDetails) {
      this.portDetails = portDetails;
    }

    public int getInitialVersion() {
      return initialVersion;
    }

    public void setInitialVersion(int version) {
      this.initialVersion = version;
    }

    public int getCurrentVersion() {
      return currentVersion;
    }

    public void setCurrentVersion(int version) {
      this.currentVersion = version;
    }

    @Override
    public String toString() {
      return "DatanodeDetailsYaml(" + uuid + ", " + hostName + "/" + ipAddress + ")";
    }
  }

  private static DatanodeDetailsYaml getDatanodeDetailsYaml(
      DatanodeDetails datanodeDetails, ConfigurationSource conf)
      throws IOException {

    DatanodeLayoutStorage datanodeLayoutStorage
        = new DatanodeLayoutStorage(conf, datanodeDetails.getUuidString());

    Map<String, Integer> portDetails = new LinkedHashMap<>();
    final List<DatanodeDetails.Port> ports = datanodeDetails.getPorts();
    if (!CollectionUtils.isEmpty(ports)) {
      for (DatanodeDetails.Port port : ports) {
        Field f = null;
        try {
          f = DatanodeDetails.Port.Name.class
              .getDeclaredField(port.getName().name());
        } catch (NoSuchFieldException e) {
          LOG.error("There is no such field as {} in {}", port.getName().name(),
              DatanodeDetails.Port.Name.class);
        }
        if (f != null
            && f.isAnnotationPresent(BelongsToHDDSLayoutVersion.class)) {
          HDDSLayoutFeature layoutFeature
              = f.getAnnotation(BelongsToHDDSLayoutVersion.class).value();
          if (layoutFeature.layoutVersion() >
              datanodeLayoutStorage.getLayoutVersion()) {
            continue;
          }
        }
        portDetails.put(port.getName().toString(), port.getValue());
      }
    }

    String persistedOpString = null;
    if (datanodeDetails.getPersistedOpState() != null) {
      persistedOpString = datanodeDetails.getPersistedOpState().name();
    }

    return new DatanodeDetailsYaml(
        datanodeDetails.getUuidString(),
        datanodeDetails.getIpAddress(),
        datanodeDetails.getHostName(),
        datanodeDetails.getCertSerialId(),
        persistedOpString,
        datanodeDetails.getPersistedOpStateExpiryEpochSec(),
        portDetails,
        datanodeDetails.getInitialVersion(),
        datanodeDetails.getCurrentVersion());
  }
}
