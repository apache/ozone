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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.server.JsonUtils;
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

  // Pre-fetched datanode address names (address -> display name); null when not resolved via SCM
  private Map<String, String> datanodeDisplayNames = null;

  // Datanode identifiers that failed UUID-to-address resolution before RPC execution
  private final List<ResolutionFailure> resolutionFailures = new ArrayList<>();

  // Normalized hostname/address args and --node-id UUIDs for the current invocation
  private List<String> explicitAddressArgs = new ArrayList<>();
  private List<String> explicitNodeIds = new ArrayList<>();

  private static final class ResolutionFailure {
    private final String datanode;
    private final String errorMsg;

    private ResolutionFailure(String datanode, String errorMsg) {
      this.datanode = datanode;
      this.errorMsg = errorMsg;
    }
  }

  @Override
  public Void call() throws Exception {
    resolutionFailures.clear();
    datanodeDisplayNames = null;
    explicitAddressArgs = new ArrayList<>();
    explicitNodeIds = new ArrayList<>();
    resetCommandState();

    // Check if DiskBalancer is enabled in configuration
    OzoneConfiguration conf = new OzoneConfiguration();
    if (!conf.getBoolean(HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_KEY,
        HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_DEFAULT)) {
      System.err.println("Disk Balancer is not enabled. Please enable the " +
          HddsConfigKeys.HDDS_DATANODE_DISK_BALANCER_ENABLED_KEY + " configuration key.");
      return null;
    }

    explicitNodeIds.addAll(DiskBalancerSubCommandUtil.normalizeNodeIds(options.getNodeIds()));
    for (String datanodeArg : options.getDatanodes()) {
      if (DiskBalancerSubCommandUtil.isDatanodeUuid(datanodeArg)) {
        if (explicitNodeIds.isEmpty()) {
          System.err.println("Error: Datanode UUID must be specified with --node-id, not as a "
              + "positional argument. For multiple UUIDs use a comma-separated list, for example "
              + "--node-id uuid1,uuid2 or --node-id \"uuid1, uuid2\".");
          return null;
        }
        explicitNodeIds.add(datanodeArg);
      } else {
        explicitAddressArgs.add(datanodeArg);
      }
    }

    // Validate that either datanode addresses, --node-id, or --in-service-datanodes is specified
    if (explicitAddressArgs.isEmpty()
        && explicitNodeIds.isEmpty()
        && !options.isInServiceDatanodes()) {
      System.err.println("Error: Either datanode address(es), --node-id, or --in-service-datanodes "
          + "must be specified.");
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
    if (targetDatanodes == null) {
      targetDatanodes = new ArrayList<>();
    }
    if (targetDatanodes.isEmpty() && resolutionFailures.isEmpty()) {
      System.err.println("Error: No datanodes found to execute command on.");
      return null;
    }

    // Remove duplicates while preserving order
    List<String> deduplicatedDatanodes = targetDatanodes.stream()
        .distinct()
        .collect(Collectors.toList());

    // Track if we're using batch mode for display
    isBatchMode = options.isInServiceDatanodes();

    // Execute on all target datanodes and collect results
    List<String> successNodes = new ArrayList<>();
    List<String> failedNodes = new ArrayList<>();
    List<Object> jsonResults = new ArrayList<>();

    for (ResolutionFailure resolutionFailure : resolutionFailures) {
      failedNodes.add(resolutionFailure.datanode);
      if (options.isJson()) {
        jsonResults.add(createErrorResult(resolutionFailure.datanode, resolutionFailure.errorMsg));
      } else {
        System.err.printf("Error on node [%s]: %s%n",
            formatDatanodeDisplayName(resolutionFailure.datanode), resolutionFailure.errorMsg);
      }
    }
    
    // Execute commands and collect results
    for (String dn : deduplicatedDatanodes) {
      try {
        Object result = executeCommand(dn);
        successNodes.add(dn);
        if (options.isJson()) {
          jsonResults.add(result);
        }
      } catch (Exception e) {
        failedNodes.add(dn);
        String errorMsg = e.getMessage();
        if (errorMsg != null && errorMsg.contains("\n")) {
          errorMsg = errorMsg.split("\n", 2)[0];
        }
        if (errorMsg == null || errorMsg.isEmpty()) {
          errorMsg = e.getClass().getSimpleName();
        }
        if (options.isJson()) {
          // Create error result object in JSON format
          Map<String, Object> errorResult = createErrorResult(dn, errorMsg);
          jsonResults.add(errorResult);
        } else {
          // Print error messages in non-JSON mode
          System.err.printf("Error on node [%s]: %s%n", formatDatanodeDisplayName(dn), errorMsg);
        }
      }
    }
    
    // Output results
    if (options.isJson()) {
      if (!jsonResults.isEmpty()) {
        System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(jsonResults));
      }
    } else {
      displayResults(successNodes, failedNodes);
    }
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
   * Get the common options for this command.
   * @return the DiskBalancerCommonOptions instance
   */
  protected DiskBalancerCommonOptions getOptions() {
    return options;
  }

  /**
   * Get the list of target datanodes to execute the command on.
   * Either from positional arguments, --node-id, or by querying SCM for in-service datanodes.
   */
  private List<String> getTargetDatanodes() {
    if (options.isInServiceDatanodes()) {
      return getAllInServiceDatanodes();
    }
    return resolveExplicitDatanodeTargets(explicitAddressArgs, explicitNodeIds);
  }

  /**
   * Resolves hostname/host:port arguments and --node-id UUIDs to CLIENT_RPC addresses.
   * Hostname and host:port arguments are passed through unchanged without contacting SCM.
   */
  private List<String> resolveExplicitDatanodeTargets(
      List<String> addressArgs, List<String> nodeIdArgs) {
    List<String> resolvedAddresses = new ArrayList<>(addressArgs);
    if (nodeIdArgs.isEmpty()) {
      datanodeDisplayNames = null;
      return resolvedAddresses;
    }

    Map<String, String> displayNames = new LinkedHashMap<>();
    ScmClient scmClient;
    try {
      scmClient = new ContainerOperationClient(new OzoneConfiguration());
    } catch (IOException e) {
      String msg = e.getMessage();
      System.err.printf("Error resolving datanode address(es).%n%s%n", msg);
      nodeIdArgs.forEach(nodeId -> addResolutionFailure(nodeId, msg));
      datanodeDisplayNames = null;
      return addressArgs;
    }

    try {
      for (String nodeId : nodeIdArgs) {
        try {
          DiskBalancerSubCommandUtil.DatanodeTarget target =
              DiskBalancerSubCommandUtil.resolveDatanodeTargetByUuid(scmClient, nodeId);
          resolvedAddresses.add(target.getClientRpcAddress());
          displayNames.put(target.getClientRpcAddress(), target.getDisplayName());
        } catch (IOException e) {
          addResolutionFailure(nodeId, e.getMessage());
        }
      }
    } finally {
      try {
        scmClient.close();
      } catch (IOException e) {
        System.err.printf("Error closing SCM client after resolving datanode address(es).%n%s%n",
            e.getMessage());
      }
    }

    datanodeDisplayNames = displayNames.isEmpty() ? null : displayNames;
    return resolvedAddresses;
  }

  private void addResolutionFailure(String datanode, String errorMsg) {
    for (ResolutionFailure existing : resolutionFailures) {
      if (existing.datanode.equals(datanode)) {
        return;
      }
    }
    resolutionFailures.add(new ResolutionFailure(datanode, errorMsg));
  }

  /**
   * Query SCM for all in-service datanodes and return their CLIENT_RPC addresses.
   */
  private List<String> getAllInServiceDatanodes() {
    try (ScmClient scmClient = new ContainerOperationClient(new OzoneConfiguration())) {
      datanodeDisplayNames = DiskBalancerSubCommandUtil.getAllOperableNodesClientRpcAddress(scmClient);
      return new ArrayList<>(datanodeDisplayNames.keySet());
    } catch (IOException e) {
      System.err.printf("Error querying SCM for in-service datanodes. %n%s%n", e.getMessage());
      return null;
    }
  }

  /**
   * Reset subcommand-specific state before each invocation.
   */
  protected void resetCommandState() {
    // Default: no subcommand-specific state
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
   * Return a JSON-serializable object if successful.
   * The base class handles whether to use it for JSON output or not.
   *
   * @param hostName the hostName in "host:port" format
   * @return result object for JSON serialization (must not be null)
   * @throws Exception if execution fails
   */
  protected abstract Object executeCommand(String hostName) throws Exception;

  /**
   * Display consolidated results after executing on all datanodes.
   * For JSON mode, this may be called for summary purposes only.
   * 
   * @param successNodes list of nodes where command succeeded
   * @param failedNodes list of nodes where command failed
   */
  protected abstract void displayResults(List<String> successNodes, List<String> failedNodes);

  /**
   * Get the action name for this command (e.g., "start", "stop", "update", "status", "report").
   * Used for creating error result objects in JSON format.
   * 
   * @return the action name
   */
  protected abstract String getActionName();

  /**
   * Get the configuration map for this command, if any configuration was provided.
   * Used for creating error result objects in JSON format.
   * Returns null if no configuration was provided.
   * 
   * @return configuration map or null
   */
  protected Map<String, Object> getConfigurationMap() {
    // Default: no configuration
    return null;
  }

  /**
   * Create an error result object in JSON format.
   * 
   * @param datanode the datanode address
   * @param errorMsg the error message
   * @return error result map
   */
  private Map<String, Object> createErrorResult(String datanode, String errorMsg) {
    Map<String, Object> errorResult = new LinkedHashMap<>();
    // Format datanode string with hostname if available
    String formattedDatanode = formatDatanodeDisplayName(datanode);
    errorResult.put("datanode", formattedDatanode);
    errorResult.put("action", getActionName());
    errorResult.put("status", "failure");
    errorResult.put("errorMsg", errorMsg);
    
    // Include configuration if it was provided
    Map<String, Object> configMap = getConfigurationMap();
    if (configMap != null && !configMap.isEmpty()) {
      errorResult.put("configuration", configMap);
    }
    
    return errorResult;
  }

  /**
   * Format a datanode address for display using pre-fetched SCM metadata when available.
   * Batch mode and --node-id populate {@link #datanodeDisplayNames}.
   * Hostname and host:port arguments without SCM resolution are returned as-is.
   *
   * @param address the datanode CLIENT_RPC address or unresolved user input
   * @return UUID when resolved via --node-id, hostname (ip:port) in batch mode, or address as-is
   */
  protected String formatDatanodeDisplayName(String address) {
    if (datanodeDisplayNames != null) {
      return datanodeDisplayNames.getOrDefault(address, address);
    }
    return address;
  }

  /**
   * Format a datanode for status/report output.
   * Uses SCM-enriched display names when available; otherwise shows hostname (ip:port) without UUID.
   *
   * @param address the datanode CLIENT_RPC address used for command execution
   * @param nodeProto datanode details from the DiskBalancer RPC response
   * @return formatted datanode identifier for output
   */
  protected String formatDatanodeDisplayName(
      String address, HddsProtos.DatanodeDetailsProto nodeProto) {
    if (datanodeDisplayNames != null && datanodeDisplayNames.containsKey(address)) {
      return datanodeDisplayNames.get(address);
    }
    return DiskBalancerSubCommandUtil.getDatanodeHostAndIp(nodeProto);
  }
}

