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
import org.apache.hadoop.ozone.om.helpers.ListOpenFilesResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Handler of ozone admin om listopenfiles command.
 */
@CommandLine.Command(
    name = "listopenfiles",
    aliases = {"listopenkeys", "lof", "lok"},
    description = "Lists open files (keys) in Ozone Manager.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class ListOpenFilesSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID",
      required = false
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"-host", "--service-host"},
      description = "Ozone Manager Host. If OM HA is enabled, use -id instead. "
          + "If insists on using -host with OM HA, this must point directly "
          + "to the leader OM. "
          + "This option is required when -id is not provided or "
          + "when HA is not enabled."
  )
  private String omHost;

  @CommandLine.Option(names = { "--json" },
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  // TODO: Use {@link org.apache.hadoop.ozone.shell.ListOptions}?
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
  private long limit;

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
      System.err.println("Error: Please specify -id or -host");
      return null;
    }

    OzoneManagerProtocol ozoneManagerClient =
        parent.createOmClient(omServiceId, omHost, false);

    ListOpenFilesResult res =
        ozoneManagerClient.listOpenFiles(pathPrefix, limit, startItem);

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
    // TODO: Untested
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(res));
  }

  private void printOpenKeysList(ListOpenFilesResult res) {

    List<OpenKeySession> openFileList = res.getOpenFiles();

    // TODO: Conform to HDFS style output?
    String msg = res.getGlobalTotal() + " global open files (estimated). " +
        "Showing " + openFileList.size() + " open files (limit " + limit + ") " +
        "under path prefix:\n  " + pathPrefix;

    if (startItem != null && !startItem.isEmpty()) {
      msg += "\nafter continuation token:\n  " + startItem;
    }
    msg += "\n\nClient ID\t\tHsync'ed\tPath";  // TODO: Add creation time
    System.out.println(msg);

    for (OpenKeySession e : openFileList) {
      long clientId = e.getId();
      String line = clientId + "\t";
      OmKeyInfo omKeyInfo = e.getKeyInfo();

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
      } else {
        line += "No\t\t";
      }

      System.out.println(line + getFullPathFromKeyInfo(omKeyInfo)
//          + "\t" + omKeyInfo  // TODO: Remove full KeyInfo print
      );
    }

    // Compose next batch's command
    if (res.hasMore()) {
      OpenKeySession lastElement = openFileList.get(openFileList.size() - 1);
      String nextBatchCmd =
          getCmdForNextBatch(getFullPathFromKeyInfo(lastElement.getKeyInfo()));

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
    if (!omServiceId.isEmpty()) {
      nextBatchCmd += " -id=" + omServiceId;
    }
    if (!omHost.isEmpty()) {
      nextBatchCmd += " -host=" + omHost;
    }
    if (json) {
      nextBatchCmd += " --json";
    }
    nextBatchCmd += " --length=" + limit;
    if (!pathPrefix.isEmpty()) {
      nextBatchCmd += " --prefix=" + pathPrefix;
    }
    if (!startItem.isEmpty()) {
      nextBatchCmd += " --start=" + lastElementFullPath;
    }
    return nextBatchCmd;
  }

  private String getFullPathFromKeyInfo(OmKeyInfo oki) {
    return "/" + oki.getVolumeName() +
        "/" + oki.getBucketName() +
        "/" + oki.getPath();
  }

}
