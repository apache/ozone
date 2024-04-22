/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.repair;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import picocli.CommandLine;

import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;

/**
 * Ozone Repair Command line tool.
 */
@CommandLine.Command(name = "ozone repair",
    description = "Operational tool to repair Ozone",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class OzoneRepair extends GenericCli {

  public static final String WARNING_SYS_USER_MESSAGE =
      "ATTENTION: You are currently logged in as user '%s'. Ozone typically runs as user '%s'." +
          " If you proceed with this command, it may change the ownership of RocksDB files " +
          " used by the Ozone Manager (OM). This ownership change could prevent OM from starting successfully." +
          " Are you sure you want to continue (y/N)? ";


  private OzoneConfiguration ozoneConf;

  public OzoneRepair() {
    super(OzoneRepair.class);
  }

  @VisibleForTesting
  public OzoneRepair(OzoneConfiguration configuration) {
    super(OzoneRepair.class);
    this.ozoneConf = configuration;
  }

  public OzoneConfiguration getOzoneConf() {
    if (ozoneConf == null) {
      ozoneConf = createOzoneConfiguration();
    }
    return ozoneConf;
  }

  /**
   * Main for the Ozone Repair shell Command handling.
   *
   * @param argv - System Args Strings[]
   * @throws Exception
   */
  public static void main(String[] argv) throws Exception {
    new OzoneRepair().run(argv);
  }

  @Override
  public int execute(String[] argv) {
    String currentUser = getSystemUserName();
    boolean shouldProceed = true;
    if (!currentUser.equals(OZONE_DEFAULT_USER)) {
      String s = getConsoleReadLineWithFormat(currentUser, OZONE_DEFAULT_USER);
      shouldProceed = Boolean.parseBoolean(s) || "y".equalsIgnoreCase(s);
    }
    if (!shouldProceed) {
      System.out.println("Aborting command.");
      return 1;
    }
    System.out.println("Run as user: " + currentUser);

    TracingUtil.initTracing("shell", createOzoneConfiguration());
    String spanName = "ozone repair " + String.join(" ", argv);
    return TracingUtil.executeInNewSpan(spanName,
        () -> super.execute(argv));
  }

  public  String getSystemUserName() {
    return System.getProperty("user.name");
  }

  public  String getConsoleReadLineWithFormat(String currentUser, String defaultUser) {
    return System.console().readLine(String.format(WARNING_SYS_USER_MESSAGE, currentUser, defaultUser));
  }

}
