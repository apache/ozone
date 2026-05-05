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

package org.apache.hadoop.hdds.scm.cli.container;

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import picocli.CommandLine;

/**
 * @deprecated by {@code ozone repair datanode upgrade-container-schema}
 */
@CommandLine.Command(
    name = "upgrade",
    description = "Please see `ozone repair datanode upgrade-container-schema`.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
@Deprecated
public class UpgradeSubcommand extends AbstractSubcommand implements Callable<Void> {

  @CommandLine.Option(names = {"--volume"}, description = "ignored")
  private String volume;

  @CommandLine.Option(names = {"-y", "--yes"}, description = "ignored")
  private boolean yes;

  @Override
  public Void call() throws Exception {
    throw new IllegalStateException(
        "This command was moved, please use it via `ozone repair datanode upgrade-container-schema` instead.");
  }
}
