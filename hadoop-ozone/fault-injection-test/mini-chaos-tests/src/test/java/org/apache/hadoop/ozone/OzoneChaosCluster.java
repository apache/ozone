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

package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import picocli.CommandLine;

/**
 * Main driver class for Ozone Chaos Cluster
 * This has multiple sub implementations of chaos cluster as options.
 */
@CommandLine.Command(
    name = "chaos",
    description = "Starts IO with MiniOzoneChaosCluster",
    subcommands = {
        TestAllMiniChaosOzoneCluster.class,
        TestDatanodeMiniChaosOzoneCluster.class,
        TestOzoneManagerMiniChaosOzoneCluster.class,
        TestStorageContainerManagerMiniChaosOzoneCluster.class
    },
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class OzoneChaosCluster extends GenericCli {
  public static void main(String[] args) {
    new OzoneChaosCluster().run(args);
  }
}
