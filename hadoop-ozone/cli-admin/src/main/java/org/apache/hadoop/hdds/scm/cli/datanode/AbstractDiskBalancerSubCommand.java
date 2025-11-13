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

package org.apache.hadoop.hdds.scm.cli.datanode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

/**
 * Abstract base class for DiskBalancer subcommands.
 */
@CommandLine.Command
public abstract class AbstractDiskBalancerSubCommand implements Callable<Void> {
  
  @CommandLine.Mixin
  private DiskBalancerCommonOptions options;

  // Track if we're in batch mode to run commands on all in-service datanodes
  private boolean isBatchMode = false;

  @Override
  public Void call() throws Exception {
    // Check if DiskBalancer is enabled in configuration
    OzoneConfiguration conf = new OzoneConfiguration();
    if (!conf.getBoolean(HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_KEY,
        HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_DEFAULT)) {
      System.err.println("Disk Balancer is not enabled. Please enable the " +
          HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_KEY + " configuration key.");
      return null;
    }

    // Validate that either -d or --in-service-datanodes is specified
    if ((options.getDatanodes() == null || options.getDatanodes().isEmpty()) 
        && !options.isInServiceDatanodes()) {
      System.err.println("Error: Either -d/--datanodes or --in-service-datanodes must be specified.");
      return null;
    }

    // Validate parameters before executing
    String validationError = validateParameters();
    if (validationError != null) {
      System.err.printf("Error: %s%n", validationError);
      return null;
    }

    // Get the list of datanodes to execute on
    List<String> targetDatanodes = getTargetDatanodes();
    if (targetDatanodes == null || targetDatanodes.isEmpty()) {
      System.err.println("Error: No datanodes found to execute command on.");
      return null;
    }

    // Track if we're using batch mode for display
    isBatchMode = options.isInServiceDatanodes();

    // Execute on all target datanodes and collect results
    List<String> successNodes = new ArrayList<>();
    List<String> failedNodes = new ArrayList<>();
    
    for (String dn : targetDatanodes) {
      if (executeCommand(dn)) {
        successNodes.add(dn);
      } else {
        failedNodes.add(dn);
      }
    }
    
    // Display consolidated results
    displayResults(successNodes, failedNodes);
    return null;
  }

  /**
   * Check if the command is running in batch mode (--in-service-datanodes).
   * @return true if batch mode, false otherwise
   */
  protected boolean isBatchMode() {
    return isBatchMode;
  }

  /**
   * Get the list of target datanodes to execute the command on.
   * Either from -d option or by querying SCM for in-service datanodes.
   */
  private List<String> getTargetDatanodes() {
    if (options.isInServiceDatanodes()) {
      return getAllInServiceDatanodes();
    } else {
      return options.getDatanodes();
    }
  }

  /**
   * Query SCM for all in-service datanodes and return their CLIENT_RPC addresses.
   */
  private List<String> getAllInServiceDatanodes() {
    try (ScmClient scmClient = new ContainerOperationClient(new OzoneConfiguration())) {
      return DiskBalancerSubCommandUtil.getAllOperableNodesClientRpcAddress(scmClient);
    } catch (IOException e) {
      System.err.println("Error querying SCM for in-service datanodes: %n" + e.getMessage());
      return null;
    }
  }

  /**
   * Validate command parameters before execution.
   * 
   * @return error message if validation fails, null if validation succeeds
   */
  protected String validateParameters() {
    // Default: no validation
    return null;
  }

  /**
   * Execute the DiskBalancer command on a single hostName.
   *
   * @param hostName the hostName in "host:port" format
   * @return true if successful, false if failed
   */
  protected abstract boolean executeCommand(String hostName);

  /**
   * Display consolidated results after executing on all datanodes.
   * 
   * @param successNodes list of nodes where command succeeded
   * @param failedNodes list of nodes where command failed
   */
  protected abstract void displayResults(List<String> successNodes, List<String> failedNodes);
}

