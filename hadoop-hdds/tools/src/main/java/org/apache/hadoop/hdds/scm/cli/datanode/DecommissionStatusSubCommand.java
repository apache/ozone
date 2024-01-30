/**
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import picocli.CommandLine;

import javax.net.ssl.HttpsURLConnection;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_BIND_HOST_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTPS_BIND_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_BIND_PORT_DEFAULT;
import static org.apache.hadoop.hdds.server.http.HttpConfig.getHttpPolicy;
import static org.apache.hadoop.hdds.server.http.HttpServer2.HTTPS_SCHEME;
import static org.apache.hadoop.hdds.server.http.HttpServer2.HTTP_SCHEME;

/**
 * Handler to print decommissioning nodes status.
 */
@CommandLine.Command(
    name = "decommission",
    description = "Show status of datanodes in DECOMMISSIONING",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)

public class DecommissionStatusSubCommand extends ScmSubcommand {

  @CommandLine.ParentCommand
  private StatusSubCommand parent;

  @CommandLine.Option(names = { "--id" },
      description = "Show info by datanode UUID",
      defaultValue = "")
  private String uuid;

  @CommandLine.Option(names = { "--ip" },
      description = "Show info by datanode ipAddress",
      defaultValue = "")
  private String ipAddress;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<HddsProtos.Node> decommissioningNodes;
    Stream<HddsProtos.Node> allNodes = scmClient.queryNode(DECOMMISSIONING,
        null, HddsProtos.QueryScope.CLUSTER, "").stream();
    if (!Strings.isNullOrEmpty(uuid)) {
      decommissioningNodes = allNodes.filter(p -> p.getNodeID().getUuid()
          .equals(uuid)).collect(Collectors.toList());
      if (decommissioningNodes.isEmpty()) {
        System.err.println("Datanode: " + uuid + " is not in DECOMMISSIONING");
        return;
      }
    } else if (!Strings.isNullOrEmpty(ipAddress)) {
      decommissioningNodes = allNodes.filter(p -> p.getNodeID().getIpAddress()
          .compareToIgnoreCase(ipAddress) == 0).collect(Collectors.toList());
      if (decommissioningNodes.isEmpty()) {
        System.err.println("Datanode: " + ipAddress + " is not in " +
            "DECOMMISSIONING");
        return;
      }
    } else {
      decommissioningNodes = allNodes.collect(Collectors.toList());
      System.out.println("\nDecommission Status: DECOMMISSIONING - " +
          decommissioningNodes.size() + " node(s)");
    }

    Map<String, Object> counts = getCounts();
    int numDecomNodes;
    Double num = (Double) counts.get("DecommissioningMaintenanceNodesTotal");
    if (num == null) {
      numDecomNodes = -1;
    } else {
      numDecomNodes = num.intValue();
    }
    for (HddsProtos.Node node : decommissioningNodes) {
      DatanodeDetails datanode = DatanodeDetails.getFromProtoBuf(
          node.getNodeID());
      printDetails(datanode);
      printCounts(datanode, counts, numDecomNodes);
      Map<String, List<ContainerID>> containers = scmClient.getContainersOnDecomNode(datanode);
      System.out.println(containers);
    }
  }
  private void printDetails(DatanodeDetails datanode) {
    System.out.println("\nDatanode: " + datanode.getUuid().toString() +
        " (" + datanode.getNetworkLocation() + "/" + datanode.getIpAddress()
        + "/" + datanode.getHostName() + ")");
  }
  private void printCounts(DatanodeDetails datanode, Map<String, Object> counts, int numDecomNodes) {
    try {
      for (int i = 1; i <= numDecomNodes; i++) {
        if (datanode.getHostName().equals(counts.get("tag.datanode." + i))) {
          int pipelines = ((Double)counts.get("PipelinesWaitingToCloseDN." + i)).intValue();
          int underReplicated = ((Double)counts.get("UnderReplicatedDN." + i)).intValue();
          int unclosed = ((Double)counts.get("UnclosedContainersDN." + i)).intValue();
          long startTime = ((Double)counts.get("StartTimeDN." + i)).longValue();
          System.out.print("Decommission started at : ");
          Date date = new Date(startTime);
          DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss z");
          System.out.println(formatter.format(date));
          System.out.println("No. of Pipelines: " + pipelines);
          System.out.println("No. of UnderReplicated containers: " + underReplicated);
          System.out.println("No. of Unclosed Containers: " + unclosed);
          return;
        }
      }
      System.err.println("Error getting pipeline and container counts for " + datanode.getHostName());
    } catch (NullPointerException ex) {
      System.err.println("Error getting pipeline and container counts for " + datanode.getHostName());
    }
  }

  private Map<String, Object> getCounts() {
    Map<String, Object> finalResult = new HashMap<>();
    try {
      StringBuffer url = new StringBuffer();
      final OzoneConfiguration ozoneConf = parent
          .getParent()
          .getParent()
          .getOzoneConf();
      final String protocol;
      final URLConnectionFactory connectionFactory = URLConnectionFactory.newDefaultURLConnectionFactory(ozoneConf);
      final HttpConfig.Policy webPolicy = getHttpPolicy(ozoneConf);
      String host;
      InputStream inputStream;
      int errorCode;

      if (webPolicy.isHttpsEnabled()) {
        protocol = HTTPS_SCHEME;
        host = ozoneConf.get(OZONE_SCM_HTTPS_ADDRESS_KEY,
            OZONE_SCM_HTTP_BIND_HOST_DEFAULT + OZONE_SCM_HTTPS_BIND_PORT_DEFAULT);
        url.append(protocol).append("://").append(host).append("/jmx")
            .append("?qry=Hadoop:service=StorageContainerManager,name=NodeDecommissionMetrics");

        HttpsURLConnection httpsURLConnection = (HttpsURLConnection) connectionFactory
            .openConnection(new URL(url.toString()));
        httpsURLConnection.connect();
        errorCode = httpsURLConnection.getResponseCode();
        inputStream = httpsURLConnection.getInputStream();
      } else {
        protocol = HTTP_SCHEME;
        host = ozoneConf.get(OZONE_SCM_HTTP_ADDRESS_KEY,
            OZONE_SCM_HTTP_BIND_HOST_DEFAULT + ":" + OZONE_SCM_HTTP_BIND_PORT_DEFAULT);
        url.append(protocol + "://" + host).append("/jmx")
            .append("?qry=Hadoop:service=StorageContainerManager,name=NodeDecommissionMetrics");

        HttpURLConnection httpURLConnection = (HttpURLConnection) connectionFactory
            .openConnection(new URL(url.toString()));
        httpURLConnection.connect();
        errorCode = httpURLConnection.getResponseCode();
        inputStream = httpURLConnection.getInputStream();
      }

      if ((errorCode == HTTP_OK) || (errorCode == HTTP_CREATED)) {
        String response = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        HashMap<String, ArrayList<Map>> result = new Gson().fromJson(response, HashMap.class);
        finalResult = result.get("beans").get(0);
        return finalResult;
      } else {
        throw new IOException("Unable to retrieve pipeline and container counts.");
      }
    } catch (MalformedURLException ex) {
      System.err.println("Unable to retrieve pipeline and container counts.");
      return finalResult;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public StatusSubCommand getParent() {
    return parent;
  }

  @VisibleForTesting
  public void setParent(OzoneConfiguration conf) {
    parent = new StatusSubCommand();
    parent.setParent(conf);
  }

}
