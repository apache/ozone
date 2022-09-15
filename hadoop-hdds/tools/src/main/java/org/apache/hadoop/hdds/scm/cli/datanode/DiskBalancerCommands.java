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
package org.apache.hadoop.hdds.scm.cli.datanode;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import java.util.concurrent.Callable;

/**
 * Subcommand to group disk balancer related operations.
 *
 * <p>The balancer is a tool that balances space usage on an Ozone datanode
 * when some disks become full or when new empty disks were added to a datanode.
 *
 * <p>SYNOPSIS
 * <pre>
 * To start:
 *      ozone admin datanode diskbalancer start
 *      [ -t/--threshold {@literal <threshold>}]
 *      [ -b/--bandwidthInMB {@literal <bandwidthInMB>}]
 *      [ -p/--parallelThread {@literal <parallelThread>}]
 *      [ -a/--alldatanodes {@literal <alldatanodes>}]
 *      [ {@literal <hosts>}]
 *      Examples:
 *      ozone admin datanode diskbalancer start {@literal <hosts>}
 *        start balancer with default values in the configuration on specified
 *        datanodes
 *      ozone admin datanode diskbalancer start -a
 *        start balancer with default values in the configuration on all
 *        datanodes in the cluster
 *      ozone admin datanode diskbalancer start -t 5 {@literal <hosts>}
 *        start balancer with a threshold of 5%
 *      ozone admin datanode diskbalancer start -b 20 {@literal <hosts>}
 *        start balancer with maximum 20MB/s diskbandwidth
 *      ozone admin datanode diskbalancer start -p 5 {@literal <hosts>}
 *        start balancer with 5 parallel thread on each datanode
 * To stop:
 *      ozone admin datanode diskbalancer stop -a
 *        stop diskblancer on all datanodes
 *      ozone admin datanode diskbalancer stop {@literal <hosts>};
 *        stop diskblancer on all datanodes
 * To update:
 *      ozone admin datanode diskbalancer update -a
 *        update diskblancer configuration on all datanodes
 *      ozone admin datanode diskbalancer update {@literal <hosts>};
 *        update diskblancer configuration on all datanodes
 * To get report:
 *      ozone admin datanode diskbalancer report -c 10
 *        retrieve at most 10 datanodes that needs diskbalance most
 * To get status:
 *      ozone admin datanode diskbalancer status -s RUNNING {@literal <hosts>}
 *        return the diskbalancer status on datanodes where diskbalancer are in
 *        Running state
 *
 * </pre>
 */

@Command(
    name = "diskbalancer",
    description = "DiskBalancer specific operations",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        DiskBalancerStartSubcommand.class,
        DiskBalancerStopSubcommand.class,
        DiskBalancerUpdateSubcommand.class,
        DiskBalancerReportSubcommand.class,
        DiskBalancerStatusSubcommand.class
    })
public class DiskBalancerCommands implements Callable<Void> {

  @Spec
  private CommandSpec spec;

  @Override
  public Void call() throws Exception {
    GenericCli.missingSubcommand(spec);
    return null;
  }
}
