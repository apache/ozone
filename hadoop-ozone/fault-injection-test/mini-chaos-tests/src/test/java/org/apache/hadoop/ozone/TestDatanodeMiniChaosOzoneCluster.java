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
import org.apache.hadoop.ozone.loadgenerators.RandomLoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.AgedLoadGenerator;

import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Test Datanode with Chaos.
 */
@CommandLine.Command(
    name = "dn",
    description = "run chaos cluster across Ozone Datanodes",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class TestDatanodeMiniChaosOzoneCluster extends
    TestMiniChaosOzoneCluster implements Callable<Void> {

  @Override
  public Void call() throws Exception {
    addLoadClasses(RandomLoadGenerator.class);
    addLoadClasses(AgedLoadGenerator.class);

    addFailureClasses(Failures.DatanodeStartStopFailure.class);
    addFailureClasses(Failures.DatanodeRestartFailure.class);

    startChaosCluster();

    return null;
  }

}
