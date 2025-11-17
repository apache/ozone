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

package org.apache.hadoop.ozone.freon;

import static org.apache.hadoop.hdds.server.http.HttpServer2.setHttpBaseDir;

import java.io.IOException;
import org.apache.hadoop.hdds.cli.ExtensibleParentCommand;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Ozone data generator and performance test tool.
 */
@Command(
    name = "ozone freon",
    description = "Load generator and tester tool for ozone",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class Freon extends GenericCli implements ExtensibleParentCommand {

  private static final Logger LOG = LoggerFactory.getLogger(Freon.class);

  @Option(names = "--server",
      description = "Enable internal http server to provide metric "
          + "and profile endpoint")
  private boolean httpServer = false;

  private final boolean interactive = System.console() != null;

  private FreonHttpServer freonHttpServer;
  private OzoneConfiguration conf;

  @Override
  public int execute(String[] argv) {
    conf = getOzoneConf();
    HddsServerUtil.initializeMetrics(conf, "ozone-freon");
    TracingUtil.initTracing("freon", conf);
    return super.execute(argv);
  }

  @Override
  public Class<?> subcommandType() {
    return FreonSubcommand.class;
  }

  public void stopHttpServer() {
    if (freonHttpServer != null) {
      try {
        freonHttpServer.stop();
      } catch (Exception e) {
        LOG.error("Freon http server can't be stopped", e);
      }
    }
  }

  public void startHttpServer() {
    if (httpServer) {
      try {
        setHttpBaseDir(conf);
        freonHttpServer = new FreonHttpServer(conf);
        freonHttpServer.start();
      } catch (IOException e) {
        LOG.error("Freon http server can't be started", e);
      }
    }

  }

  public static void main(String[] args) {
    new Freon().run(args);
  }

  public boolean isInteractive() {
    return interactive;
  }
}
