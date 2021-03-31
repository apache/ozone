/*
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

import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;

import static org.apache.hadoop.hdds.utils.HAUtils.checkSecurityAndSCMHAEnabled;

/**
 * This class provides a command line interface to start the OM
 * using Picocli.
 */
@Command(name = "ozone om",
    hidden = true, description = "Start or initialize the Ozone Manager.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class OzoneManagerStarter extends GenericCli {

  private OzoneConfiguration conf;
  private OMStarterInterface receiver;
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerStarter.class);

  public static void main(String[] args) throws Exception {
    new OzoneManagerStarter(
        new OzoneManagerStarter.OMStarterHelper()).run(args);
  }

  public OzoneManagerStarter(OMStarterInterface receiverObj) {
    receiver = receiverObj;
  }

  @Override
  public Void call() throws Exception {
    /**
     * This method is invoked only when a sub-command is not called. Therefore
     * if someone runs "ozone om" with no parameters, this is the method
     * which runs and starts the OM.
     */
    try {
      commonInit();
      startOm();
    } catch (Exception ex) {
      LOG.error("OM start failed with exception", ex);
      throw ex;
    }
    return null;
  }

  /**
   * This function is used by the command line to start the OM.
   */
  private void startOm() throws Exception {
    receiver.start(conf);
  }

  /**
   * This function implements a sub-command to allow the OM to be
   * initialized from the command line.
   */
  @CommandLine.Command(name = "--init",
      customSynopsis = "ozone om [global options] --init",
      hidden = false,
      description = "Initialize the Ozone Manager if not already initialized",
      mixinStandardHelpOptions = true,
      versionProvider = HddsVersionProvider.class)
  public void initOm()
      throws Exception {
    commonInit();
    boolean result = receiver.init(conf);
    if (!result) {
      throw new IOException("OM Init failed.");
    }
  }

  /**
   * This function implements a sub-command to allow the OM to be
   * Removed from prepare mode after an upgrade or downgrade.
   */
  @CommandLine.Command(name = "--upgrade",
      aliases = "--downgrade",
      customSynopsis = "ozone om [global options] --upgrade",
      description = "Cancels prepare state in this OM on startup",
      mixinStandardHelpOptions = true,
      versionProvider = HddsVersionProvider.class)
  public void startOmUpgrade() throws Exception {
    try {
      commonInit();
      receiver.startAndCancelPrepare(conf);
    } catch (Exception ex) {
      LOG.error("Cancelling prepare to start OM in upgrade mode failed " +
          "with exception", ex);
      throw ex;
    }
  }

  /**
   * This function should be called by each command to ensure the configuration
   * is set and print the startup banner message.
   */
  private void commonInit() {
    conf = createOzoneConfiguration();
    TracingUtil.initTracing("OzoneManager", conf);

    String[] originalArgs = getCmd().getParseResult().originalArgs()
        .toArray(new String[0]);
    StringUtils.startupShutdownMessage(OzoneVersionInfo.OZONE_VERSION_INFO,
        OzoneManager.class, originalArgs, LOG);
  }

  /**
   * This static class wraps the external dependencies needed for this command
   * to execute its tasks. This allows the dependency to be injected for unit
   * testing.
   */
  static class OMStarterHelper implements OMStarterInterface{

    @Override
    public void start(OzoneConfiguration conf) throws IOException,
        AuthenticationException {
      checkSecurityAndSCMHAEnabled(conf);
      OzoneManager om = OzoneManager.createOm(conf);
      om.start();
      om.join();
    }

    @Override
    public boolean init(OzoneConfiguration conf) throws IOException,
        AuthenticationException {
      checkSecurityAndSCMHAEnabled(conf);
      return OzoneManager.omInit(conf);
    }

    @Override
    public void startAndCancelPrepare(OzoneConfiguration conf)
        throws IOException, AuthenticationException {
      OzoneManager om = OzoneManager.createOm(conf);
      om.getPrepareState().cancelPrepare();
      om.start();
      om.join();
    }
  }
}