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

package org.apache.hadoop.ozone.admin.reconfig;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocolPB.ReconfigureProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Reconfigure subcommand utils.
 */
final class ReconfigureSubCommandUtil {

  private ReconfigureSubCommandUtil() {
  }

  public static ReconfigureProtocol getSingleNodeReconfigureProxy(
      HddsProtos.NodeType nodeType, String address) throws IOException {
    OzoneConfiguration ozoneConf = new OzoneConfiguration();
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    InetSocketAddress nodeAddr = NetUtils.createSocketAddr(address);
    return new ReconfigureProtocolClientSideTranslatorPB(nodeType,
        nodeAddr, user, ozoneConf);
  }

  public static <T> void parallelExecute(ExecutorService executorService,
      List<T> nodes, BiConsumer<HddsProtos.NodeType, T> operation) {
    AtomicInteger successCount = new AtomicInteger();
    AtomicInteger failCount = new AtomicInteger();
    if (nodes != null) {
      for (T node : nodes) {
        executorService.submit(() -> {
          try {
            operation.accept(HddsProtos.NodeType.DATANODE, node);
            successCount.incrementAndGet();
          } catch (Exception e) {
            failCount.incrementAndGet();
            e.printStackTrace(System.out);
          }
        });
      }
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(3, TimeUnit.MINUTES)) {
          System.out.println(
              "Couldn't terminate executor in 180s.");
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.out.println("Executor termination interrupted");
      } finally {
        System.out.printf("Reconfig successfully %d nodes, failure %d nodes.%n",
            successCount.get(), failCount.get());
      }
    }
  }

  public static List<String> getAllOperableNodesClientRpcAddress(
      ScmClient scmClient) throws IOException {
    List<HddsProtos.Node> nodes = scmClient.queryNode(
        NodeOperationalState.IN_SERVICE, null,
        HddsProtos.QueryScope.CLUSTER, "");

    List<String> addresses = new ArrayList<>();
    for (HddsProtos.Node node : nodes) {
      DatanodeDetails details =
          DatanodeDetails.getFromProtoBuf(node.getNodeID());
      if (node.getNodeStates(0).equals(HddsProtos.NodeState.DEAD)) {
        continue;
      }
      Port port = details.getPort(Port.Name.CLIENT_RPC);
      if (port != null) {
        addresses.add(details.getIpAddress() + ":" + port.getValue());
      } else {
        System.out.printf("host: %s(%s) %s port not found",
            details.getHostName(), details.getIpAddress(),
            Port.Name.CLIENT_RPC.name());
      }
    }

    return addresses;
  }

}
