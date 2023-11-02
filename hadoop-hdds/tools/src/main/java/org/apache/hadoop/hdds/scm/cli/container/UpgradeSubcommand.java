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
package org.apache.hadoop.hdds.scm.cli.container;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.container.upgrade.UpgradeChecker;
import org.apache.hadoop.hdds.scm.cli.container.upgrade.UpgradeManager;
import org.apache.hadoop.hdds.scm.cli.container.upgrade.UpgradeUtils;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Callable;

/**
 * This is the handler that process container upgrade command.
 */
@Command(
    name = "upgrade",
    description = "Upgrade schema v2 containers to v3",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class UpgradeSubcommand implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(UpgradeSubcommand.class);

  @CommandLine.Option(names = {"--volume"},
      required = false,
      description = "volume name")
  private String volume;

  @CommandLine.Option(names = {"-y", "--yes"},
      description = "Continue without interactive user confirmation")
  private boolean yes;

  private static OzoneConfiguration ozoneConfiguration;


  @Override
  public Void call() throws Exception {
    final UpgradeChecker upgradeChecker = new UpgradeChecker();

    OzoneConfiguration configuration = getConfiguration();

    Pair<Boolean, String> pair =
        upgradeChecker.checkDatanodeNotStarted();
    final boolean dnNotStarted = pair.getKey();
    if (!dnNotStarted) {
      printAndExit(pair.getValue());
    }

    final Pair<Integer, Integer> layoutVersion =
        upgradeChecker.getLayoutVersion(configuration);
    final Integer softwareLayoutVersion = layoutVersion.getLeft();
    final Integer metadataLayoutVersion = layoutVersion.getRight();
    final int needLayoutVersion =
        HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion();

    if (metadataLayoutVersion < needLayoutVersion ||
        softwareLayoutVersion < needLayoutVersion) {
      printAndExit(String.format(
          "Please upgrade your software version, no less than %s," +
              " current metadata layout version is %d," +
              " software layout version is %d",
          HDDSLayoutFeature.DATANODE_SCHEMA_V3.name(),
          metadataLayoutVersion, softwareLayoutVersion));
    }

    if (!Strings.isNullOrEmpty(volume)) {
      configuration.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, volume);
    }

    DatanodeDetails dnDetail =
        UpgradeUtils.getDatanodeDetails(configuration);

    final HddsProtos.NodeOperationalState opState =
        dnDetail.getPersistedOpState();

    if (!opState.equals(HddsProtos.NodeOperationalState.IN_MAINTENANCE)) {
      printAndExit("This command requires the datanode's " +
          "NodeOperationalState to be IN_MAINTENANCE, currently is " +
          opState);
    }

    List<HddsVolume> allVolume =
        upgradeChecker.getAllVolume(dnDetail, configuration);

    final List<File> volumeDBPath = upgradeChecker.getVolumeDBPath(allVolume);

    if (volumeDBPath.isEmpty()) {
      printAndExit("No volume db store exists, need finalizae data node.");
    }

    for (HddsVolume hddsVolume : allVolume) {
      if (UpgradeChecker.checkAlreadyMigrate(hddsVolume)) {
        printAndExit("Volume " + hddsVolume.getVolumeRootDir() +
            " it's already upgraded, skip it.");
      }
    }

    if (!yes) {
      Scanner scanner = new Scanner(new InputStreamReader(
          System.in, StandardCharsets.UTF_8));
      System.out.println(
          "The volume db store, will be automatically backup," +
              " should you continue to upgrade?");
      boolean confirm = scanner.next().trim().equals("yes");
      scanner.close();
      if (!confirm) {
        return null;
      }
    }

    final Map<File, Exception> backupExceptions =
        upgradeChecker.dbBackup(volumeDBPath);

    if (!backupExceptions.isEmpty()) {
      final String backupFailDbPath = Arrays.toString(
          backupExceptions.keySet().stream().map(File::getAbsolutePath)
              .distinct().toArray());

      printAndExit("Db store path: " + backupFailDbPath +
          " backup fail, please check.");
    }

    // do upgrade
    final UpgradeManager upgradeManager = new UpgradeManager();
    upgradeManager.run(configuration);
    return null;
  }

  @VisibleForTesting
  public static void setOzoneConfiguration(OzoneConfiguration config) {
    ozoneConfiguration = config;
  }

  private OzoneConfiguration getConfiguration() {
    return ozoneConfiguration;
  }

  private static void printAndExit(String str) {
    ExitUtils.terminate(-1, str, LOG);
  }
}
