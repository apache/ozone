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
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;

/**
 * This is the handler that process container upgrade command.
 */
@Command(
    name = "upgrade",
    description = "Offline upgrade all schema V2 containers to schema V3 " +
        "for this datanode.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class UpgradeSubcommand implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(UpgradeSubcommand.class);

  @CommandLine.Spec
  private static CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(names = {"--volume"},
      required = false,
      description = "volume path")
  private String volume;

  @CommandLine.Option(names = {"-y", "--yes"},
      description = "Continue without interactive user confirmation")
  private boolean yes;

  private static OzoneConfiguration ozoneConfiguration;


  @Override
  public Void call() throws Exception {
    OzoneConfiguration configuration = getConfiguration();
    // Verify admin privilege
    OzoneAdmins admins = OzoneAdmins.getOzoneAdmins("", configuration);
    if (!admins.isAdmin(UserGroupInformation.getCurrentUser())) {
      out().println("It requires ozone administrator privilege. Current user" +
          " is " + UserGroupInformation.getCurrentUser() + ".");
      return null;
    }

    final UpgradeChecker upgradeChecker = new UpgradeChecker();
    Pair<Boolean, String> pair = upgradeChecker.checkDatanodeRunning();
    final boolean isRunning = pair.getKey();
    if (isRunning) {
      out().println(pair.getValue());
      return null;
    }

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
      out().println(String.format(
          "Please upgrade your software version, no less than %s," +
              " current metadata layout version is %s," +
              " software layout version is %s",
          HDDSLayoutFeature.DATANODE_SCHEMA_V3.name(),
          metadataLayoutFeature.name(), softwareLayoutFeature.name()));
      return null;
    }

    if (!Strings.isNullOrEmpty(volume)) {
      File volumeDir = new File(volume);
      if (!volumeDir.exists() || !volumeDir.isDirectory()) {
        out().println(
            String.format("Volume path %s is not a directory or doesn't exist",
                volume));
        return null;
      }
      File hddsRootDir = new File(volume + "/" + HddsVolume.HDDS_VOLUME_DIR);
      File versionFile = new File(volume + "/" + HddsVolume.HDDS_VOLUME_DIR +
          "/" + Storage.STORAGE_FILE_VERSION);
      if (!hddsRootDir.exists() || !hddsRootDir.isDirectory() ||
          !versionFile.exists() || !versionFile.isFile()) {
        out().println(
            String.format("Volume path %s is not a valid data volume", volume));
        return null;
      }
      configuration.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, volume);
    }

    final HddsProtos.NodeOperationalState opState =
        dnDetail.getPersistedOpState();

    if (!opState.equals(HddsProtos.NodeOperationalState.IN_MAINTENANCE)) {
      out().println("This command requires the datanode's " +
          "NodeOperationalState to be IN_MAINTENANCE, currently is " +
          opState);
      return null;
    }

    List<HddsVolume> allVolume =
        upgradeChecker.getAllVolume(dnDetail, configuration);

    Iterator<HddsVolume> volumeIterator = allVolume.iterator();
    while (volumeIterator.hasNext()) {
      HddsVolume hddsVolume = volumeIterator.next();
      if (UpgradeChecker.isAlreadyUpgraded(hddsVolume)) {
        out().println("Volume " + hddsVolume.getVolumeRootDir() +
            " is already upgraded, skip it.");
        volumeIterator.remove();
      }
    }

    if (allVolume.isEmpty()) {
      out().println("There is no more volume to upgrade. Exit.");
      return null;
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
        return null;
      }
    }

    // do upgrade
    final UpgradeManager upgradeManager = new UpgradeManager();
    upgradeManager.run(configuration, allVolume);
    return null;
  }

  @VisibleForTesting
  public static void setOzoneConfiguration(OzoneConfiguration config) {
    ozoneConfiguration = config;
  }

  private OzoneConfiguration getConfiguration() {
    if (ozoneConfiguration == null) {
      ozoneConfiguration = new OzoneConfiguration();
    }
    return ozoneConfiguration;
  }

  private static PrintWriter err() {
    return spec.commandLine().getErr();
  }

  private static PrintWriter out() {
    return spec.commandLine().getOut();
  }
}
