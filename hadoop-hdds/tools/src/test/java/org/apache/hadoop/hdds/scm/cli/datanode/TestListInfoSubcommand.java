package org.apache.hadoop.hdds.scm.cli.datanode;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.MockScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests to validate the the TestListInfoSubCommand class includes the
 * correct output when executed against a mock client.
 */
public class TestListInfoSubcommand {

  private Client scmClient;
  private ListInfoSubcommand cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  @Before
  public void setup() {
    scmClient = new Client();
    cmd = new ListInfoSubcommand();
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @After
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testDataNodeOperationalStateIncludedInOutput() throws Exception {
    cmd.execute(scmClient);
    // The output should contain a string like:
    // <other lines>
    // Operational State: <STATE>
    // <other lines>
    Pattern p = Pattern.compile(
        "^Operational State:\\s+IN_SERVICE$", Pattern.MULTILINE);
    Matcher m = p.matcher(outContent.toString());
    assertTrue(m.find());
    // Should also have a node with the state DECOMMISSIONING
    p = Pattern.compile(
        "^Operational State:\\s+DECOMMISSIONING$", Pattern.MULTILINE);
    m = p.matcher(outContent.toString());
    assertTrue(m.find());
  }

  private static class Client extends MockScmClient {

    @Override
    /**
     * Returns 2 nodes - one IN_SERVICE and one DECOMMISSIONING
     */
    public List<HddsProtos.Node> queryNode(
        HddsProtos.NodeOperationalState opState, HddsProtos.NodeState nodeState,
        HddsProtos.QueryScope queryScope, String poolName) {
      List<HddsProtos.Node> nodes = new ArrayList<>();

      for (int i=0; i<2; i++) {
        HddsProtos.DatanodeDetailsProto.Builder dnd =
            HddsProtos.DatanodeDetailsProto.newBuilder();
        dnd.setHostName("host" + i);
        dnd.setIpAddress("1.2.3." + i+1);
        dnd.setNetworkLocation("/default");
        dnd.setNetworkName("host" + i);
        dnd.addPorts(HddsProtos.Port.newBuilder()
            .setName("ratis").setValue(5678).build());
        dnd.setUuid(UUID.randomUUID().toString());

        HddsProtos.Node.Builder builder  = HddsProtos.Node.newBuilder();
        if (i == 0) {
          builder.addNodeOperationalStates(
              HddsProtos.NodeOperationalState.IN_SERVICE);
        } else {
          builder.addNodeOperationalStates(
              HddsProtos.NodeOperationalState.DECOMMISSIONING);
        }
        builder.addNodeStates(HddsProtos.NodeState.HEALTHY);
        builder.setNodeID(dnd.build());
        nodes.add(builder.build());
      }
      return nodes;
    }

    @Override
    public List<Pipeline> listPipelines() {
      return new ArrayList<>();
    }

  }
}