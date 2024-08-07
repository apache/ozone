/**
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

package org.apache.hadoop.ozone.admin.om;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.ListOpenFilesResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Handler of ozone admin om list-open-files command.
 */
@CommandLine.Command(
    name = "list-open-files",
    aliases = {"list-open-keys", "lof", "lok"},
    description = "Lists open files (keys) in Ozone Manager.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class ListOpenFilesSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(
      names = {"--service-id", "--om-service-id"},
      description = "Ozone Manager Service ID",
      required = false
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"--service-host"},
      description = "Ozone Manager Host. If OM HA is enabled, use --service-id instead. "
          + "If you must use --service-host with OM HA, this must point directly to the leader OM. "
          + "This option is required when --service-id is not provided or when HA is not enabled."
  )
  private String omHost;

  @CommandLine.Option(names = { "--json" },
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  @CommandLine.Option(names = { "--show-deleted" },
      defaultValue = "false",
      description = "Whether to show deleted open keys")
  private boolean showDeleted;

  @CommandLine.Option(names = { "--show-overwritten" },
      defaultValue = "false",
      description = "Whether to show overwritten open keys")
  private boolean showOverwritten;

  // Conforms to ListOptions, but not all in ListOptions applies here thus
  // not using that directly
  @CommandLine.Option(
      names = {"-p", "--prefix"},
      description = "Filter results by the specified path on the server side.",
      defaultValue = "/"
  )
  private String pathPrefix;

  @CommandLine.Option(
      names = {"-l", "--length"},
      description = "Maximum number of items to list",
      defaultValue = "100"
  )
  private int limit;

  @CommandLine.Option(
      names = {"-s", "--start"},
      description = "The item to start the listing from.\n" +
          "i.e. continuation token. " +
          "This will be excluded from the result.",
      defaultValue = ""
  )
  private String startItem;

  @Override
  public Void call() throws Exception {

    if (StringUtils.isEmpty(omServiceId) && StringUtils.isEmpty(omHost)) {
      System.err.println("Error: Please specify --service-id or --service-host");
      return null;
    }

    OzoneManagerProtocol ozoneManagerClient =
        parent.createOmClient(omServiceId, omHost, false);
    ServiceInfoEx serviceInfoEx = ozoneManagerClient.getServiceInfo();
    final OzoneManagerVersion omVersion = RpcClient.getOmVersion(serviceInfoEx);
    if (omVersion.compareTo(OzoneManagerVersion.HBASE_SUPPORT) < 0) {
      System.err.println("Error: This command requires OzoneManager version "
          + OzoneManagerVersion.HBASE_SUPPORT.name() + " or later.");
      return null;
    }

    ListOpenFilesResult res =
        ozoneManagerClient.listOpenFiles(pathPrefix, limit, startItem);

    if (!showDeleted) {
      res.getOpenKeys().removeIf(o -> o.getKeyInfo().getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY));
    }
    if (!showOverwritten) {
      res.getOpenKeys().removeIf(o -> o.getKeyInfo().getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY));
    }
    if (json) {
      // Print detailed JSON
      printOpenKeysListAsJson(res);
    } else {
      // Human friendly output
      printOpenKeysList(res);
    }

    return null;
  }

  private void printOpenKeysListAsJson(ListOpenFilesResult res)
      throws IOException {
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(res));
  }

  private void printOpenKeysList(ListOpenFilesResult res) {

    List<OpenKeySession> openFileList = res.getOpenKeys();

    String msg = res.getTotalOpenKeyCount() +
        " total open files (est.). Showing " + openFileList.size() +
        " open files (limit " + limit + ") under path prefix:\n  " + pathPrefix;

    if (startItem != null && !startItem.isEmpty()) {
      msg += "\nafter continuation token:\n  " + startItem;
    }
    msg += "\n\nClient ID\t\t\tCreation time\t\tHsync'ed\t";
    msg += showDeleted ? "Deleted\t" : "";
    msg += showOverwritten ? "Overwritten\t" : "";
    msg += "Open File Path";
    System.out.println(msg);

    for (OpenKeySession e : openFileList) {
      long clientId = e.getId();
      OmKeyInfo omKeyInfo = e.getKeyInfo();
      String line = clientId + "\t" + Instant.ofEpochMilli(omKeyInfo.getCreationTime()) + "\t";

      if (omKeyInfo.isHsync()) {
        String hsyncClientIdStr =
            omKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
        long hsyncClientId = Long.parseLong(hsyncClientIdStr);
        if (clientId == hsyncClientId) {
          line += "Yes\t\t";
        } else {
          // last hsync'ed with a different client ID than the client that
          // initially opens the file (!)
          line += "Yes w/ cid " + hsyncClientIdStr + "\t";
        }

        if (showDeleted) {
          if (omKeyInfo.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY)) {
            line += "Yes\t\t";
          } else {
            line += "No\t\t";
          }
        }
        if (showOverwritten) {
          if (omKeyInfo.getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY)) {
            line += "Yes\t";
          } else {
            line += "No\t";
          }
        }
      } else {
        line += showDeleted ? "No\t\tNo\t\t" : "No\t\t";
        line += showOverwritten ? "No\t" : "";
      }

      line += getFullPathFromKeyInfo(omKeyInfo);

      System.out.println(line);
    }

    // Compose next batch's command
    if (res.hasMore()) {
      String nextBatchCmd = getCmdForNextBatch(res.getContinuationToken());

      System.out.println("\n" +
          "To get the next batch of open keys, run:\n  " + nextBatchCmd);
    } else {
      System.out.println("\nReached the end of the list.");
    }
  }

  /**
   * @return the command to get the next batch of open keys
   */
  private String getCmdForNextBatch(String lastElementFullPath) {
    String nextBatchCmd = "ozone admin om lof";
    if (omServiceId != null && !omServiceId.isEmpty()) {
      nextBatchCmd += " -id=" + omServiceId;
    }
    if (omHost != null && !omHost.isEmpty()) {
      nextBatchCmd += " -host=" + omHost;
    }
    if (json) {
      nextBatchCmd += " --json";
    }
    nextBatchCmd += " --length=" + limit;
    if (pathPrefix != null && !pathPrefix.isEmpty()) {
      nextBatchCmd += " --prefix=" + pathPrefix;
    }
    nextBatchCmd += " --start=" + lastElementFullPath;
    return nextBatchCmd;
  }

  private String getFullPathFromKeyInfo(OmKeyInfo oki) {
    return "/" + oki.getVolumeName() +
        "/" + oki.getBucketName() +
        "/" + oki.getPath();
  }

}
