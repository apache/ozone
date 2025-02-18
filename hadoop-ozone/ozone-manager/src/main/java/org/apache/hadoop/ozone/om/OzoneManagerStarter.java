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

import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;

import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.util.OzoneNetUtils;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * This class provides a command line interface to start the OM
 * using Picocli.
 */
@Command(name = "ozone om",
    hidden = true, description = "Start or initialize the Ozone Manager.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class OzoneManagerStarter extends GenericCli implements Callable<Void> {

  private OzoneConfiguration conf;
  private OMStarterInterface receiver;
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerStarter.class);

  public static void main(String[] args) throws Exception {
    OzoneNetUtils.disableJvmNetworkAddressCacheIfRequired(
            new OzoneConfiguration());
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
   * This function implements a sub-command to allow the OM to be bootstrapped
   * from the command line.
   *
   * During initialization, OM will get the metadata information from all the
   * other OMs to check whether their on disk configs have been updated with
   * this new OM information. If not, the bootstrap step will fail. This
   * check is skipped with the --force option.
   * Note that if an OM does not have updated configs, it can crash when a
   * force bootstrap is initiated. The force option is provided for the
   * scenario where one of the old OMs is down or not responding and the
   * bootstrap needs to continue.
   *
   * Bootstrapping OM will request the leader OM to add itself to the ring.
   * Once the leader OM responds back affirmatively, bootstrap step is
   * complete and the OM is functional.
   */
  @CommandLine.Command(name = "--bootstrap",
      customSynopsis = "ozone om [global options] --bootstrap [options]",
      hidden = false,
      description = "Initializes and Bootstraps the Ozone Manager.",
      mixinStandardHelpOptions = true,
      versionProvider = HddsVersionProvider.class)
  public void bootstrapOM(@CommandLine.Option(names = {"--force"},
      description = "This option will skip checking whether existing OMs " +
          "configs have been updated with the new OM information. Force " +
          "bootstrap can cause an existing OM to crash if it does not have " +
          "updated configs. It should only be used if an existing OM is down " +
          "or not responding and after manually checking that the ozone-site" +
          ".xml config is updated on all OMs.",
      defaultValue = "false") boolean force)
      throws Exception {
    commonInit();
    receiver.bootstrap(conf, force);
  }

  /**
   * This function should be called by each command to ensure the configuration
   * is set and print the startup banner message.
   */
  private void commonInit() {
    conf = getOzoneConf();
    TracingUtil.initTracing("OzoneManager", conf);

    String[] originalArgs = getCmd().getParseResult().originalArgs()
        .toArray(new String[0]);
    HddsServerUtil.startupShutdownMessage(OzoneVersionInfo.OZONE_VERSION_INFO,
        OzoneManager.class, originalArgs, LOG, conf);
  }

  /**
   * This static class wraps the external dependencies needed for this command
   * to execute its tasks. This allows the dependency to be injected for unit
   * testing.
   */
  static class OMStarterHelper implements OMStarterInterface {

    @Override
    public void start(OzoneConfiguration conf) throws IOException,
        AuthenticationException {
      OzoneManager om = OzoneManager.createOm(conf);
      om.start();
      ShutdownHookManager.get().addShutdownHook(() -> {
        try {
          if (om.stop()) {
            om.join();
          }
        } catch (Exception e) {
          LOG.error("Error during stop OzoneManager.", e);
        }
      }, DEFAULT_SHUTDOWN_HOOK_PRIORITY);
    }

    @Override
    public boolean init(OzoneConfiguration conf) throws IOException,
        AuthenticationException {
      return OzoneManager.omInit(conf);
    }

    @Override
    public void bootstrap(OzoneConfiguration conf, boolean force)
        throws IOException, AuthenticationException {
      // Initialize the Ozone Manager, if not already initialized.
      boolean initialize = OzoneManager.omInit(conf);
      if (!initialize) {
        throw new IOException("OM Init failed.");
      }
      OzoneManager.StartupOption startupOption;
      if (force) {
        startupOption = OzoneManager.StartupOption.FORCE_BOOTSTRAP;
      } else {
        startupOption = OzoneManager.StartupOption.BOOTSTRAP;
      }
      // Bootstrap the OM
      try (OzoneManager om = OzoneManager.createOm(conf, startupOption)) {
        om.start();
        om.join();
      }
    }

    @Override
    public void startAndCancelPrepare(OzoneConfiguration conf)
        throws IOException, AuthenticationException {
      try (OzoneManager om = OzoneManager.createOm(conf)) {
        om.getPrepareState().cancelPrepare();
        om.start();
        om.join();
      }
    }
  }
}
