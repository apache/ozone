/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.failure.Failures;
import org.apache.hadoop.ozone.loadgenerators.*;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Command line utility to parse and dump a datanode ratis segment file.
 */
@CommandLine.Command(
    name = "all",
    description = "run chaos cluster across all daemons",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class TestAllMiniChaosOzoneCluster extends TestMiniChaosOzoneCluster
    implements Callable<Void> {

  @CommandLine.ParentCommand
  private OzoneChaosCluster chaosCluster;

  @Override
  public Void call() throws Exception {
    setNumManagers(3, 3, true);

    LoadGenerator.getClassList().forEach(
        TestMiniChaosOzoneCluster::addLoadClasses);
    Failures.getClassList().forEach(
        TestMiniChaosOzoneCluster::addFailureClasses);

    startChaosCluster();

    return null;
  }

}
