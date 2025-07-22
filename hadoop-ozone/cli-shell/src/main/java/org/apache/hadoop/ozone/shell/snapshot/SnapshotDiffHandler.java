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

package org.apache.hadoop.ozone.shell.snapshot;

import static org.apache.hadoop.hdds.server.JsonUtils.toJsonStringWithDefaultPrettyPrinter;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.bucket.BucketUri;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import picocli.CommandLine;

/**
 * ozone sh snapshot diff.
 */
@CommandLine.Command(name = "diff", aliases = "snapshotDiff",
    description = "Get the differences between two snapshots")
public class SnapshotDiffHandler extends Handler {

  @CommandLine.Mixin
  private BucketUri snapshotPath;

  @CommandLine.Parameters(description = "from snapshot name",
      index = "1")
  private String fromSnapshot;

  @CommandLine.Parameters(description = "to snapshot name",
      index = "2")
  private String toSnapshot;

  @CommandLine.Option(names = {"-t", "--token"},
      description = "continuation token for next page (optional)")
  private String token;

  @CommandLine.Option(names = {"-p", "--page-size"},
      description = "number of diff entries to be returned in the response. " +
          "Note the effective page size will also be bound by " +
          "the server-side page size limit, see config:\n" +
          "  ozone.om.snapshot.diff.max.page.size",
      defaultValue = "1000",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS
  )
  private int pageSize;

  @CommandLine.Option(names = {"--ffd", "--force-full-diff"},
      description = "perform full diff of snapshot without using" +
          " optimised DAG based pruning approach (optional)",
      hidden = true)
  private boolean forceFullDiff;

  @CommandLine.Option(names = {"-c", "--cancel"},
      description = "Request to cancel a running SnapshotDiff job. " +
          "If the job is not IN_PROGRESS, the request will fail.",
      defaultValue = "false")
  private boolean cancel;

  @CommandLine.Option(names = {"--dnld", "--disable-native-libs-diff"},
      description = "perform diff of snapshot without using" +
          " native libs (optional)",
      hidden = true)
  private boolean diffDisableNativeLibs;

  @CommandLine.Option(names = { "--json" },
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  @Override
  protected OzoneAddress getAddress() {
    return snapshotPath.getValue();
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = snapshotPath.getValue().getVolumeName();
    String bucketName = snapshotPath.getValue().getBucketName();
    OmUtils.validateSnapshotName(fromSnapshot);
    OmUtils.validateSnapshotName(toSnapshot);

    if (cancel) {
      cancelSnapshotDiff(client.getObjectStore(), volumeName, bucketName);
    } else {
      getSnapshotDiff(client.getObjectStore(), volumeName, bucketName);
    }
  }

  private void getSnapshotDiff(ObjectStore store, String volumeName,
                               String bucketName) throws IOException {
    SnapshotDiffResponse diffResponse = store.snapshotDiff(volumeName, bucketName, fromSnapshot, toSnapshot,
        token, pageSize, forceFullDiff, diffDisableNativeLibs);
    try (PrintWriter writer = out()) {
      if (json) {
        writer.println(toJsonStringWithDefaultPrettyPrinter(getJsonObject(diffResponse)));
      } else {
        writer.println(diffResponse);
      }
    }
  }

  private void cancelSnapshotDiff(ObjectStore store, String volumeName,
                                  String bucketName) throws IOException {
    try (PrintWriter writer = out()) {
      writer.println(store.cancelSnapshotDiff(volumeName, bucketName, fromSnapshot, toSnapshot));
    }
  }

  private ObjectNode getJsonObject(SnapshotDiffResponse diffResponse) {
    ObjectNode diffResponseNode = JsonUtils.createObjectNode(null);
    diffResponseNode.set("snapshotDiffReport", getJsonObject(diffResponse.getSnapshotDiffReport()));
    diffResponseNode.put("jobStatus", diffResponse.getJobStatus().name());
    if (diffResponse.getWaitTimeInMs() != 0) {
      diffResponseNode.put("waitTimeInMs", diffResponse.getWaitTimeInMs());
    }
    if (StringUtils.isNotEmpty(diffResponse.getReason())) {
      diffResponseNode.put("reason", diffResponse.getReason());
    }
    return diffResponseNode;
  }

  private ObjectNode getJsonObject(SnapshotDiffReportOzone diffReportOzone) {
    ObjectNode diffReportNode = JsonUtils.createObjectNode(null);
    diffReportNode.put("volumeName", diffReportOzone.getVolumeName());
    diffReportNode.put("bucketName", diffReportOzone.getBucketName());
    diffReportNode.put("fromSnapshot", diffReportOzone.getFromSnapshot());
    diffReportNode.put("toSnapshot", diffReportOzone.getLaterSnapshotName());
    ArrayNode resArray = JsonUtils.createArrayNode();
    diffReportOzone.getDiffList()
        .forEach(diffReportEntry -> resArray.add(getJsonObject(diffReportEntry)));
    diffReportNode.set("diffList", resArray);

    if (StringUtils.isNotEmpty(diffReportOzone.getToken())) {
      diffReportNode.put("nextToken", diffReportOzone.getToken());
    }
    return diffReportNode;
  }

  private ObjectNode getJsonObject(SnapshotDiffReport.DiffReportEntry diffReportEntry) {
    ObjectNode diffReportNode = JsonUtils.createObjectNode(null);
    diffReportNode.put("diffType", diffReportEntry.getType().getLabel());
    diffReportNode.put("sourcePath", getPathString(diffReportEntry.getSourcePath()));
    if (diffReportEntry.getTargetPath() != null) {
      diffReportNode.put("targetPath", getPathString(diffReportEntry.getTargetPath()));
    }
    return diffReportNode;
  }

  private String getPathString(byte[] path) {
    String pathStr = DFSUtilClient.bytes2String(path);
    if (pathStr.isEmpty()) {
      return Path.CUR_DIR;
    } else {
      return Path.CUR_DIR + Path.SEPARATOR + pathStr;
    }
  }
}
