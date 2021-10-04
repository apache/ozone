package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestECBlockOutputStreamEntry {

  @Test
  public void
  testAcquireDifferentClientForECBlocksOnTheSameHostButDifferentPort()
      throws IOException {
    PipelineID randomId = PipelineID.randomId();
    ReplicationConfig ecReplicationConfig =
        new ECReplicationConfig("RS-3-2-1024k");
    DatanodeDetails node1 = aNode("127.0.0.1", "localhost", 2001);
    DatanodeDetails node2 = aNode("127.0.0.1", "localhost", 2002);
    DatanodeDetails node3 = aNode("127.0.0.1", "localhost", 2003);
    DatanodeDetails node4 = aNode("127.0.0.1", "localhost", 2004);
    DatanodeDetails node5 = aNode("127.0.0.1", "localhost", 2005);
    List<DatanodeDetails> nodes =
        Arrays.asList(node1, node2, node3, node4, node5);
    Pipeline anECPipeline = Pipeline.newBuilder()
        .setId(randomId)
        .setReplicationConfig(ecReplicationConfig)
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(nodes)
        .build();
    XceiverClientManager manager =
        new XceiverClientManager(new OzoneConfiguration());
    HashSet<XceiverClientSpi> clients = new HashSet<>();
    ECBlockOutputStreamEntry entry = new ECBlockOutputStreamEntry.Builder()
        .setXceiverClientManager(manager)
        .setPipeline(anECPipeline)
        .build();
    for (int i = 0; i < nodes.size(); i++) {
      clients.add(
          manager.acquireClient(
              entry.createSingleECBlockPipeline(
                  anECPipeline, nodes.get(i), i
              )));
    }
    assertEquals(5, clients.size());
  }

  @Test
  public void
  testAcquireDifferentClientForECBlocksOnTheSameHostWithSomeOnSamePortAlso()
      throws IOException {
    PipelineID randomId = PipelineID.randomId();
    ReplicationConfig ecReplicationConfig =
        new ECReplicationConfig("RS-3-2-1024k");
    DatanodeDetails node1 = aNode("127.0.0.1", "localhost", 2001);
    DatanodeDetails node2 = aNode("127.0.0.1", "localhost", 2001);
    DatanodeDetails node3 = aNode("127.0.0.1", "localhost", 2003);
    DatanodeDetails node4 = aNode("127.0.0.1", "localhost", 2001);
    DatanodeDetails node5 = aNode("127.0.0.1", "localhost", 2005);
    List<DatanodeDetails> nodes =
        Arrays.asList(node1, node2, node3, node4, node5);
    Pipeline anECPipeline = Pipeline.newBuilder()
        .setId(randomId)
        .setReplicationConfig(ecReplicationConfig)
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(nodes)
        .build();
    XceiverClientManager manager =
        new XceiverClientManager(new OzoneConfiguration());
    HashSet<XceiverClientSpi> clients = new HashSet<>();
    ECBlockOutputStreamEntry entry = new ECBlockOutputStreamEntry.Builder()
        .setXceiverClientManager(manager)
        .setPipeline(anECPipeline)
        .build();
    for (int i = 0; i < nodes.size(); i++) {
      clients.add(
          manager.acquireClient(
              entry.createSingleECBlockPipeline(
                  anECPipeline, nodes.get(i), i
              )));
    }
    assertEquals(3, clients.size());
    assertEquals(1, clients.stream().filter(c -> c.getRefcount() == 3).count());
    assertEquals(2, clients.stream().filter(c -> c.getRefcount() == 1).count());
  }

  private DatanodeDetails aNode(String ip, String hostName, int port) {
    return DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setIpAddress(ip)
        .setHostName(hostName)
        .addPort(
            DatanodeDetails.newPort(DatanodeDetails.Port.Name.STANDALONE, port))
        .build();
  }
}
