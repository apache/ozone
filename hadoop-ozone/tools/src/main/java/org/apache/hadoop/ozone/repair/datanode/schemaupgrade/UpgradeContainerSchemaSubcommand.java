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

package org.apache.hadoop.ozone.repair.datanode.schemaupgrade;

import com.google.common.base.Strings;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.repair.RepairTool;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * This is the handler that process container upgrade command.
 */
@Command(
    name = "upgrade-container-schema",
    description = "Offline upgrade all schema V2 containers to schema V3 " +
        "for this datanode.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class UpgradeContainerSchemaSubcommand extends RepairTool {

  @CommandLine.Option(names = {"--volume"},
      required = false,
      description = "volume path")
  private String volume;

  @CommandLine.Option(names = {"-y", "--yes"},
      description = "Continue without interactive user confirmation")
  private boolean yes;

  @Override
  protected Component serviceToBeOffline() {
    return Component.DATANODE;
  }

  @Override
  public void execute() throws Exception {
    OzoneConfiguration configuration = getOzoneConf();

    final UpgradeChecker upgradeChecker = new UpgradeChecker();

    DatanodeDetails dnDetail =
        UpgradeUtils.getDatanodeDetails(configuration);

    Pair<HDDSLayoutFeature, HDDSLayoutFeature> layoutFeature =
        upgradeChecker.getLayoutFeature(dnDetail, configuration);
    final HDDSLayoutFeature softwareLayoutFeature = layoutFeature.getLeft();
    final HDDSLayoutFeature metadataLayoutFeature = layoutFeature.getRight();
    final int needLayoutVersion =
        HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion();

    if (metadataLayoutFeature.layoutVersion() < needLayoutVersion ||
        softwareLayoutFeature.layoutVersion() < needLayoutVersion) {
      error(
          "Please upgrade your software version, no less than %s," +
              " current metadata layout version is %s," +
              " software layout version is %s",
          HDDSLayoutFeature.DATANODE_SCHEMA_V3.name(),
          metadataLayoutFeature.name(), softwareLayoutFeature.name());
      return;
    }

    if (!Strings.isNullOrEmpty(volume)) {
      File volumeDir = new File(volume);
      if (!volumeDir.exists() || !volumeDir.isDirectory()) {
        error("Volume path %s is not a directory or doesn't exist", volume);
        return;
      }
      File hddsRootDir = new File(volume + "/" + HddsVolume.HDDS_VOLUME_DIR);
      File versionFile = new File(volume + "/" + HddsVolume.HDDS_VOLUME_DIR +
          "/" + Storage.STORAGE_FILE_VERSION);
      if (!hddsRootDir.exists() || !hddsRootDir.isDirectory() ||
          !versionFile.exists() || !versionFile.isFile()) {
        error("Volume path %s is not a valid data volume", volume);
        return;
      }
      configuration.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, volume);
    }

    final HddsProtos.NodeOperationalState opState =
        dnDetail.getPersistedOpState();

    if (!opState.equals(HddsProtos.NodeOperationalState.IN_MAINTENANCE)) {
      error("This command requires the datanode's " +
          "NodeOperationalState to be IN_MAINTENANCE, currently is " +
          opState);
      return;
    }

    List<HddsVolume> allVolume =
        upgradeChecker.getAllVolume(dnDetail, configuration);

    Iterator<HddsVolume> volumeIterator = allVolume.iterator();
    while (volumeIterator.hasNext()) {
      HddsVolume hddsVolume = volumeIterator.next();
      if (UpgradeChecker.isAlreadyUpgraded(hddsVolume)) {
        info("Volume " + hddsVolume.getVolumeRootDir() +
            " is already upgraded, skip it.");
        volumeIterator.remove();
      }
    }

    if (allVolume.isEmpty()) {
      info("There is no more volume to upgrade. Exit.");
      return;
    }

    if (!yes) {
      Scanner scanner = new Scanner(new InputStreamReader(
          System.in, StandardCharsets.UTF_8));
      System.out.println(
          "All volume db stores will be automatically backup," +
              " should we continue the upgrade ? [yes|no] : ");
      boolean confirm = scanner.next().trim().equals("yes");
      scanner.close();
      if (!confirm) {
        return;
      }
    }

    // do upgrade
    final UpgradeManager upgradeManager = new UpgradeManager();
    upgradeManager.run(configuration, allVolume);
  }
}
