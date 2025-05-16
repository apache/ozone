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

package org.apache.hadoop.hdds.scm.net;

import static org.apache.hadoop.hdds.scm.net.NetConstants.DATACENTER_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.NODEGROUP_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.NODE_COST_DEFAULT;
import static org.apache.hadoop.hdds.scm.net.NetConstants.PATH_SEPARATOR_STR;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.REGION_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test the network topology functions. */
class TestNetworkTopologyImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestNetworkTopologyImpl.class);
  private NetworkTopology cluster;
  private Node[] dataNodes;
  private final Random random = new Random();
  private Consumer<List<? extends Node>> mockedShuffleOperation;

  @BeforeEach
  void beforeAll() {
    mockedShuffleOperation = mock(Consumer.class);
    doAnswer(args -> {
          List<? extends Node> collection = args.getArgument(0);
          Collections.shuffle(collection);
          return null;
        }
    ).when(mockedShuffleOperation).accept(any());
  }

  void initNetworkTopology(NodeSchema[] schemas, Node[] nodeArray) {
    NodeSchemaManager.getInstance().init(schemas, true);
    cluster = new NetworkTopologyImpl(NodeSchemaManager.getInstance(),
        mockedShuffleOperation);
    dataNodes = nodeArray.clone();
    for (Node dataNode : dataNodes) {
      cluster.add(dataNode);
    }
  }

  static Stream<Arguments> topologies() {
    return Stream.of(
        arguments(new NodeSchema[] {ROOT_SCHEMA, LEAF_SCHEMA},
            new Node[]{
                createDatanode("1.1.1.1", "/"),
                createDatanode("2.2.2.2", "/"),
                createDatanode("3.3.3.3", "/"),
                createDatanode("4.4.4.4", "/"),
                createDatanode("5.5.5.5", "/"),
                createDatanode("6.6.6.6", "/"),
                createDatanode("7.7.7.7", "/"),
                createDatanode("8.8.8.8", "/"),
            }),
        arguments(new NodeSchema[] {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA},
            new Node[]{
                createDatanode("1.1.1.1", "/r1"),
                createDatanode("2.2.2.2", "/r1"),
                createDatanode("3.3.3.3", "/r2"),
                createDatanode("4.4.4.4", "/r2"),
                createDatanode("5.5.5.5", "/r2"),
                createDatanode("6.6.6.6", "/r3"),
                createDatanode("7.7.7.7", "/r3"),
                createDatanode("8.8.8.8", "/r3"),
            }),
        arguments(new NodeSchema[] {ROOT_SCHEMA, DATACENTER_SCHEMA, RACK_SCHEMA,
            LEAF_SCHEMA},
            new Node[]{
                createDatanode("1.1.1.1", "/d1/r1"),
                createDatanode("2.2.2.2", "/d1/r1"),
                createDatanode("3.3.3.3", "/d1/r2"),
                createDatanode("4.4.4.4", "/d1/r2"),
                createDatanode("5.5.5.5", "/d1/r2"),
                createDatanode("6.6.6.6", "/d2/r3"),
                createDatanode("7.7.7.7", "/d2/r3"),
                createDatanode("8.8.8.8", "/d2/r3"),
            }),
        arguments(new NodeSchema[] {ROOT_SCHEMA, DATACENTER_SCHEMA, RACK_SCHEMA,
            NODEGROUP_SCHEMA, LEAF_SCHEMA},
            new Node[]{
                createDatanode("1.1.1.1", "/d1/r1/ng1"),
                createDatanode("2.2.2.2", "/d1/r1/ng1"),
                createDatanode("3.3.3.3", "/d1/r2/ng2"),
                createDatanode("4.4.4.4", "/d1/r2/ng2"),
                createDatanode("5.5.5.5", "/d1/r2/ng3"),
                createDatanode("6.6.6.6", "/d2/r3/ng3"),
                createDatanode("7.7.7.7", "/d2/r3/ng3"),
                createDatanode("8.8.8.8", "/d2/r3/ng3"),
                createDatanode("9.9.9.9", "/d3/r1/ng1"),
                createDatanode("10.10.10.10", "/d3/r1/ng1"),
                createDatanode("11.11.11.11", "/d3/r1/ng1"),
                createDatanode("12.12.12.12", "/d3/r2/ng2"),
                createDatanode("13.13.13.13", "/d3/r2/ng2"),
                createDatanode("14.14.14.14", "/d4/r1/ng1"),
                createDatanode("15.15.15.15", "/d4/r1/ng1"),
                createDatanode("16.16.16.16", "/d4/r1/ng1"),
                createDatanode("17.17.17.17", "/d4/r1/ng2"),
                createDatanode("18.18.18.18", "/d4/r1/ng2"),
                createDatanode("19.19.19.19", "/d4/r1/ng3"),
                createDatanode("20.20.20.20", "/d4/r1/ng3"),
            }),
        arguments(new NodeSchema[] {ROOT_SCHEMA, REGION_SCHEMA,
            DATACENTER_SCHEMA, RACK_SCHEMA, NODEGROUP_SCHEMA, LEAF_SCHEMA},
            new Node[]{
                createDatanode("1.1.1.1", "/d1/rg1/r1/ng1"),
                createDatanode("2.2.2.2", "/d1/rg1/r1/ng1"),
                createDatanode("3.3.3.3", "/d1/rg1/r1/ng2"),
                createDatanode("4.4.4.4", "/d1/rg1/r1/ng1"),
                createDatanode("5.5.5.5", "/d1/rg1/r1/ng1"),
                createDatanode("6.6.6.6", "/d1/rg1/r1/ng2"),
                createDatanode("7.7.7.7", "/d1/rg1/r1/ng2"),
                createDatanode("8.8.8.8", "/d1/rg1/r1/ng2"),
                createDatanode("9.9.9.9", "/d1/rg1/r1/ng2"),
                createDatanode("10.10.10.10", "/d1/rg1/r1/ng2"),
                createDatanode("11.11.11.11", "/d1/rg1/r2/ng1"),
                createDatanode("12.12.12.12", "/d1/rg1/r2/ng1"),
                createDatanode("13.13.13.13", "/d1/rg1/r2/ng1"),
                createDatanode("14.14.14.14", "/d1/rg1/r2/ng1"),
                createDatanode("15.15.15.15", "/d1/rg1/r2/ng1"),
                createDatanode("16.16.16.16", "/d1/rg1/r2/ng2"),
                createDatanode("17.17.17.17", "/d1/rg1/r2/ng2"),
                createDatanode("18.18.18.18", "/d1/rg1/r2/ng2"),
                createDatanode("19.19.19.19", "/d1/rg1/r2/ng2"),
                createDatanode("20.20.20.20", "/d1/rg1/r2/ng2"),
                createDatanode("21.21.21.21", "/d2/rg1/r2/ng1"),
                createDatanode("22.22.22.22", "/d2/rg1/r2/ng1"),
                createDatanode("23.23.23.23", "/d2/rg2/r2/ng1"),
                createDatanode("24.24.24.24", "/d2/rg2/r2/ng1"),
                createDatanode("25.25.25.25", "/d2/rg2/r2/ng1"),
            })
    );
  }

  @ParameterizedTest
  @MethodSource("topologies")
  void testContains(NodeSchema[] schemas, Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    Node nodeNotInMap = createDatanode("8.8.8.8", "/d2/r4");
    for (Node dataNode : dataNodes) {
      assertTrue(cluster.contains(dataNode));
    }
    assertFalse(cluster.contains(nodeNotInMap));
  }

  @ParameterizedTest
  @MethodSource("topologies")
  void testNumOfChildren(NodeSchema[] schemas, Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    assertEquals(dataNodes.length, cluster.getNumOfLeafNode(null));
    assertEquals(0, cluster.getNumOfLeafNode("/switch1/node1"));
  }

  @ParameterizedTest
  @MethodSource("topologies")
  void testGetNode(NodeSchema[] schemas, Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    assertEquals(cluster.getNode(""), cluster.getNode(null));
    assertEquals(cluster.getNode(""), cluster.getNode("/"));
    assertNull(cluster.getNode("/switch1/node1"));
    assertNull(cluster.getNode("/switch1"));

    if (cluster.getNode("/d1") != null) {
      List<String> excludedScope = new ArrayList<>();
      excludedScope.add("/d");
      Node n = cluster.getNode(0, "/d1", excludedScope, null, null, 0);
      assertNotNull(n);
    }
  }

  @Test
  void testCreateInvalidTopology() {
    NodeSchema[] schemas =
        new NodeSchema[]{ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    NetworkTopology newCluster = new NetworkTopologyImpl(
        NodeSchemaManager.getInstance(), mockedShuffleOperation);
    Node[] invalidDataNodes = new Node[] {
        createDatanode("1.1.1.1", "/r1"),
        createDatanode("2.2.2.2", "/r2"),
        createDatanode("3.3.3.3", "/d1/r2")
    };
    newCluster.add(invalidDataNodes[0]);
    newCluster.add(invalidDataNodes[1]);
    Exception e = assertThrows(NetworkTopology.InvalidTopologyException.class,
        () -> newCluster.add(invalidDataNodes[2]));
    assertThat(e)
        .hasMessageContaining("Failed to add")
        .hasMessageContaining("Its path depth is not " + newCluster.getMaxLevel());
  }

  @Test
  void testInitWithConfigFile() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    OzoneConfiguration conf = new OzoneConfiguration();
    String filePath = classLoader
        .getResource("./networkTopologyTestFiles/good.xml")
        .getPath();
    conf.set(ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE, filePath);
    NetworkTopology newCluster = new NetworkTopologyImpl(conf);
    LOG.info("network topology max level = {}", newCluster.getMaxLevel());
    assertEquals(4, newCluster.getMaxLevel());
  }

  @ParameterizedTest
  @MethodSource("topologies")
  void testAncestor(NodeSchema[] schemas, Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    assumeTrue(cluster.getMaxLevel() > 2);
    int maxLevel = cluster.getMaxLevel();
    assertTrue(cluster.isSameParent(dataNodes[0], dataNodes[1]));
    while (maxLevel > 1) {
      assertTrue(cluster.isSameAncestor(dataNodes[0], dataNodes[1],
          maxLevel - 1));
      maxLevel--;
    }
    assertFalse(cluster.isSameParent(dataNodes[1], dataNodes[2]));
    assertFalse(cluster.isSameParent(null, dataNodes[2]));
    assertFalse(cluster.isSameParent(dataNodes[1], null));
    assertFalse(cluster.isSameParent(null, null));

    assertFalse(cluster.isSameAncestor(dataNodes[1], dataNodes[2], 0));
    assertFalse(cluster.isSameAncestor(dataNodes[1], null, 1));
    assertFalse(cluster.isSameAncestor(null, dataNodes[2], 1));
    assertFalse(cluster.isSameAncestor(null, null, 1));

    maxLevel = cluster.getMaxLevel();
    assertTrue(cluster.isSameAncestor(
        dataNodes[random.nextInt(cluster.getNumOfLeafNode(null))],
        dataNodes[random.nextInt(cluster.getNumOfLeafNode(null))],
        maxLevel - 1));
  }

  @ParameterizedTest
  @MethodSource("topologies")
  void testAddRemove(NodeSchema[] schemas, Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    for (Node dataNode : dataNodes) {
      cluster.remove(dataNode);
    }

    for (Node dataNode : dataNodes) {
      assertFalse(cluster.contains(dataNode));
    }
    // no leaf nodes
    assertEquals(0, cluster.getNumOfLeafNode(null));
    // no inner nodes
    assertEquals(0, cluster.getNumOfNodes(2));

    for (Node dataNode : dataNodes) {
      cluster.add(dataNode);
    }
    // Inner nodes are created automatically
    assertThat(cluster.getNumOfNodes(2)).isPositive();

    Exception e = assertThrows(IllegalArgumentException.class,
        () -> cluster.add(cluster.chooseRandom(null).getParent()));
    assertThat(e).hasMessageStartingWith("Not allowed to add an inner node");

    Exception e2 = assertThrows(IllegalArgumentException.class,
        () -> cluster.remove(cluster.chooseRandom(null).getParent()));
    assertThat(e2).hasMessageStartingWith("Not allowed to remove an inner node");
  }

  @ParameterizedTest
  @MethodSource("topologies")
  void testGetNumOfNodesWithLevel(NodeSchema[] schemas, Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    int maxLevel = cluster.getMaxLevel();

    Exception e = assertThrows(IllegalArgumentException.class,
        () -> cluster.getNumOfNodes(0));
    assertThat(e).hasMessageStartingWith("Invalid level");

    Exception e2 = assertThrows(IllegalArgumentException.class,
        () -> cluster.getNumOfNodes(maxLevel + 1));
    assertThat(e2).hasMessageStartingWith("Invalid level");

    // root node
    assertEquals(1, cluster.getNumOfNodes(1));
    // leaf nodes
    assertEquals(dataNodes.length, cluster.getNumOfNodes(maxLevel));
  }

  @ParameterizedTest
  @MethodSource("topologies")
  void testGetNodesWithLevel(NodeSchema[] schemas, Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    int maxLevel = cluster.getMaxLevel();

    Exception e = assertThrows(IllegalArgumentException.class,
        () -> cluster.getNodes(0));
    assertThat(e).hasMessageStartingWith("Invalid level");

    Exception e2 = assertThrows(IllegalArgumentException.class,
        () -> cluster.getNodes(maxLevel + 1));
    assertThat(e2).hasMessageStartingWith("Invalid level");

    // root node
    assertEquals(1, cluster.getNodes(1).size());
    // leaf nodes
    assertEquals(dataNodes.length, cluster.getNodes(maxLevel).size());
  }

  @ParameterizedTest
  @MethodSource("topologies")
  void testChooseRandomSimple(NodeSchema[] schemas, Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    String path =
        dataNodes[random.nextInt(dataNodes.length)].getNetworkFullPath();
    assertEquals(path, cluster.chooseRandom(path).getNetworkFullPath());
    path = path.substring(0, path.lastIndexOf(PATH_SEPARATOR_STR));
    // test chooseRandom(String scope)
    while (!path.equals(ROOT)) {
      assertThat(cluster.chooseRandom(path).getNetworkLocation())
          .startsWith(path);
      Node node = cluster.chooseRandom("~" + path);
      assertThat(node.getNetworkLocation())
          .doesNotStartWith(path);
      path = path.substring(0,
          path.lastIndexOf(PATH_SEPARATOR_STR));
    }
    assertNotNull(cluster.chooseRandom(null));
    assertNotNull(cluster.chooseRandom(""));
    assertNotNull(cluster.chooseRandom("/"));
    assertNull(cluster.chooseRandom("~"));
    assertNull(cluster.chooseRandom("~/"));

    // test chooseRandom(String scope, String excludedScope)
    path = dataNodes[random.nextInt(dataNodes.length)].getNetworkFullPath();
    List<String> pathList = new ArrayList<>();
    pathList.add(path);
    assertNull(cluster.chooseRandom(path, pathList));
    assertNotNull(cluster.chooseRandom(null, pathList));
    assertNotNull(cluster.chooseRandom("", pathList));

    // test chooseRandom(String scope, Collection<Node> excludedNodes)
    assertNull(cluster.chooseRandom("", Arrays.asList(dataNodes)));
    assertNull(cluster.chooseRandom("/", Arrays.asList(dataNodes)));
    assertNull(cluster.chooseRandom("~", Arrays.asList(dataNodes)));
    assertNull(cluster.chooseRandom("~/", Arrays.asList(dataNodes)));
    assertNull(cluster.chooseRandom(null, Arrays.asList(dataNodes)));
  }

  /**
   * Following test checks that chooseRandom works for an excluded scope.
   */
  @ParameterizedTest
  @MethodSource("topologies")
  void testChooseRandomExcludedScope(NodeSchema[] schemas,
      Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    int[] excludedNodeIndexs = {0, dataNodes.length - 1,
        random.nextInt(dataNodes.length), random.nextInt(dataNodes.length)};
    String scope;
    Map<Node, Integer> frequency;
    for (int i : excludedNodeIndexs) {
      String path = dataNodes[i].getNetworkFullPath();
      while (!path.equals(ROOT)) {
        scope = "~" + path;
        frequency = pickNodesAtRandom(100, scope, null, 0);
        for (Node key : dataNodes) {
          if (key.isDescendant(path)) {
            assertEquals(0, (int) frequency.get(key));
          }
        }
        path = path.substring(0, path.lastIndexOf(PATH_SEPARATOR_STR));
      }
    }

    // null excludedScope, every node should be chosen
    frequency = pickNodes(100, null, null, null, 0);
    for (Node key : dataNodes) {
      assertNotEquals(0, frequency.get(key));
    }

    // "" excludedScope,  no node will ever be chosen
    List<String> pathList = new ArrayList<>();
    pathList.add("");
    frequency = pickNodes(100, pathList, null, null, 0);
    for (Node key : dataNodes) {
      assertEquals(0, frequency.get(key));
    }

    // "~" scope, no node will ever be chosen
    scope = "~";
    frequency = pickNodesAtRandom(100, scope, null, 0);
    for (Node key : dataNodes) {
      assertEquals(0, frequency.get(key));
    }
    // out network topology excluded scope, every node should be chosen
    pathList.clear();
    pathList.add("/city1");
    frequency = pickNodes(
        cluster.getNumOfLeafNode(null), pathList, null, null, 0);
    for (Node key : dataNodes) {
      assertNotEquals(0, frequency.get(key));
    }
  }

  /**
   * Following test checks that chooseRandom works for an excluded nodes.
   */
  @ParameterizedTest
  @MethodSource("topologies")
  void testChooseRandomExcludedNode(NodeSchema[] schemas,
      Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    Node[][] excludedNodeLists = {
        {},
        {dataNodes[0]},
        {dataNodes[dataNodes.length - 1]},
        {dataNodes[random.nextInt(dataNodes.length)]},
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)]
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        }};
    int leafNum = cluster.getNumOfLeafNode(null);
    Map<Node, Integer> frequency;
    for (Node[] list : excludedNodeLists) {
      List<Node> excludedList = Arrays.asList(list);
      int ancestorGen = 0;
      while (ancestorGen < cluster.getMaxLevel()) {
        frequency = pickNodesAtRandom(leafNum, null, excludedList, ancestorGen);
        List<Node> ancestorList = NetUtils.getAncestorList(cluster,
            excludedList, ancestorGen);
        for (Node key : dataNodes) {
          if (excludedList.contains(key) ||
              (!ancestorList.isEmpty() &&
                  ancestorList.stream()
                      .map(a -> (InnerNode) a)
                      .anyMatch(a -> a.isAncestor(key)))) {
            assertEquals(0, frequency.get(key));
          }
        }
        ancestorGen++;
      }
    }
    // all nodes excluded, no node will be picked
    List<Node> excludedList = Arrays.asList(dataNodes);
    int ancestorGen = 0;
    while (ancestorGen < cluster.getMaxLevel()) {
      frequency = pickNodesAtRandom(leafNum, null, excludedList, ancestorGen);
      for (Node key : dataNodes) {
        assertEquals(0, frequency.get(key));
      }
      ancestorGen++;
    }
    // out scope excluded nodes, each node will be picked
    excludedList = Arrays.asList(createDatanode("1.1.1.1.", "/city1/rack1"));
    ancestorGen = 0;
    while (ancestorGen < cluster.getMaxLevel()) {
      frequency = pickNodes(leafNum, null, excludedList, null, ancestorGen);
      for (Node key : dataNodes) {
        assertNotEquals(0, frequency.get(key));
      }
      ancestorGen++;
    }
  }

  /**
   * Following test checks that chooseRandom works for excluded nodes and scope.
   */
  @ParameterizedTest
  @MethodSource("topologies")
  void testChooseRandomExcludedNodeAndScope(NodeSchema[] schemas,
      Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    int[] excludedNodeIndexs = {0, dataNodes.length - 1,
        random.nextInt(dataNodes.length), random.nextInt(dataNodes.length)};
    Node[][] excludedNodeLists = {
        {},
        {dataNodes[0]},
        {dataNodes[dataNodes.length - 1]},
        {dataNodes[random.nextInt(dataNodes.length)]},
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)]
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        }};
    int leafNum = cluster.getNumOfLeafNode(null);
    Map<Node, Integer> frequency;
    String scope;
    for (int i : excludedNodeIndexs) {
      String path = dataNodes[i].getNetworkFullPath();
      while (!path.equals(ROOT)) {
        scope = "~" + path;
        int ancestorGen = 0;
        while (ancestorGen < cluster.getMaxLevel()) {
          for (Node[] list : excludedNodeLists) {
            List<Node> excludedList = Arrays.asList(list);
            frequency =
                pickNodesAtRandom(leafNum, scope, excludedList, ancestorGen);
            List<Node> ancestorList = NetUtils.getAncestorList(cluster,
                excludedList, ancestorGen);
            for (Node key : dataNodes) {
              if (excludedList.contains(key) || key.isDescendant(path) ||
                  (!ancestorList.isEmpty() &&
                      ancestorList.stream()
                          .map(a -> (InnerNode) a)
                          .anyMatch(a -> a.isAncestor(key)))) {
                assertEquals(0, frequency.get(key));
              }
            }
          }
          ancestorGen++;
        }
        path = path.substring(0, path.lastIndexOf(PATH_SEPARATOR_STR));
      }
    }
    // all nodes excluded, no node will be picked
    List<Node> excludedList = Arrays.asList(dataNodes);
    for (int i : excludedNodeIndexs) {
      String path = dataNodes[i].getNetworkFullPath();
      while (!path.equals(ROOT)) {
        scope = "~" + path;
        int ancestorGen = 0;
        while (ancestorGen < cluster.getMaxLevel()) {
          frequency =
              pickNodesAtRandom(leafNum, scope, excludedList, ancestorGen);
          for (Node key : dataNodes) {
            assertEquals(0, frequency.get(key));
          }
          ancestorGen++;
        }
        path = path.substring(0, path.lastIndexOf(PATH_SEPARATOR_STR));
      }
    }

    // no node excluded and no excluded scope, each node will be picked
    int ancestorGen = 0;
    while (ancestorGen < cluster.getMaxLevel()) {
      frequency = pickNodes(leafNum, null, null, null, ancestorGen);
      for (Node key : dataNodes) {
        assertNotEquals(0, frequency.get(key));
      }
      ancestorGen++;
    }
  }

  /**
   * Following test checks that chooseRandom works for excluded nodes, scope
   * and ancestor generation.
   */
  @ParameterizedTest
  @MethodSource("topologies")
  void testChooseRandomWithAffinityNode(NodeSchema[] schemas,
      Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    int[] excludedNodeIndexs = {0, dataNodes.length - 1,
        random.nextInt(dataNodes.length), random.nextInt(dataNodes.length)};
    Node[][] excludedNodeLists = {
        {},
        {dataNodes[0]},
        {dataNodes[dataNodes.length - 1]},
        {dataNodes[random.nextInt(dataNodes.length)]},
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)]
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        }};
    int[] affinityNodeIndexs = {0, dataNodes.length - 1,
        random.nextInt(dataNodes.length), random.nextInt(dataNodes.length)};
    Node[][] excludedScopeIndexs = {{dataNodes[0]},
        {dataNodes[dataNodes.length - 1]},
        {dataNodes[random.nextInt(dataNodes.length)]},
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)]
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        }};
    int leafNum = cluster.getNumOfLeafNode(null);
    Map<Node, Integer> frequency;
    List<String> pathList = new ArrayList<>();
    for (int k : affinityNodeIndexs) {
      for (Node[] excludedScopes : excludedScopeIndexs) {
        pathList.clear();
        pathList.addAll(Arrays.stream(excludedScopes)
            .map(node -> node.getNetworkFullPath())
            .collect(Collectors.toList()));
        while (!pathList.get(0).equals(ROOT)) {
          int ancestorGen = cluster.getMaxLevel() - 1;
          while (ancestorGen > 0) {
            Node affinityNode = dataNodes[k];
            for (Node[] list : excludedNodeLists) {
              List<Node> excludedList = Arrays.asList(list);
              frequency = pickNodes(leafNum, pathList, excludedList,
                  affinityNode, ancestorGen);
              Node affinityAncestor = affinityNode.getAncestor(ancestorGen);
              for (Node key : dataNodes) {
                if (affinityAncestor != null) {
                  if (frequency.get(key) > 0) {
                    assertTrue(affinityAncestor.isAncestor(key));
                  } else if (!affinityAncestor.isAncestor(key)) {
                    continue;
                  } else if (excludedList != null &&
                      excludedList.contains(key)) {
                    continue;
                  } else if (pathList != null &&
                      pathList.stream().anyMatch(key::isDescendant)) {
                    continue;
                  } else if (key.getNetworkFullPath().equals(
                      affinityNode.getNetworkFullPath())) {
                    continue;
                  } else {
                    fail("Node is not picked when sequentially going " +
                        "through ancestor node's leaf nodes. node:" +
                        key.getNetworkFullPath() + ", ancestor node:" +
                        affinityAncestor.getNetworkFullPath() +
                        ", excludedScope: " + pathList + ", " +
                        "excludedList:" + excludedList);
                  }
                }
              }
            }
            ancestorGen--;
          }
          pathList = pathList.stream().map(path ->
                  path.substring(0, path.lastIndexOf(PATH_SEPARATOR_STR)))
              .collect(Collectors.toList());
        }
      }
    }

    // all nodes excluded, no node will be picked
    String scope;
    List<Node> excludedList = Arrays.asList(dataNodes);
    for (int k : affinityNodeIndexs) {
      for (int i : excludedNodeIndexs) {
        String path = dataNodes[i].getNetworkFullPath();
        while (!path.equals(ROOT)) {
          scope = "~" + path;
          int ancestorGen = 0;
          while (ancestorGen < cluster.getMaxLevel()) {
            frequency = pickNodesAtRandom(leafNum, scope, excludedList,
                dataNodes[k], ancestorGen);
            for (Node key : dataNodes) {
              assertEquals(0, frequency.get(key));
            }
            ancestorGen++;
          }
          path = path.substring(0, path.lastIndexOf(PATH_SEPARATOR_STR));
        }
      }
    }
    // no node excluded and no excluded scope, each node will be picked
    int ancestorGen = cluster.getMaxLevel() - 1;
    for (int k : affinityNodeIndexs) {
      while (ancestorGen > 0) {
        frequency =
            pickNodes(leafNum, null, null, dataNodes[k], ancestorGen);
        Node affinityAncestor = dataNodes[k].getAncestor(ancestorGen);
        for (Node key : dataNodes) {
          if (frequency.get(key) > 0) {
            if (affinityAncestor != null) {
              assertTrue(affinityAncestor.isAncestor(key));
            }
          }
        }
        ancestorGen--;
      }
    }

    // check invalid ancestor generation
    Exception e = assertThrows(IllegalArgumentException.class,
        () -> cluster.chooseRandom(null, null, null, dataNodes[0],
            cluster.getMaxLevel()));
    assertThat(e.getMessage()).startsWith("ancestorGen " + cluster.getMaxLevel() +
        " exceeds this network topology acceptable level");
  }

  @Test
  void testCost() {
    // network topology with default cost
    List<NodeSchema> schemas = new ArrayList<>();
    schemas.add(ROOT_SCHEMA);
    schemas.add(RACK_SCHEMA);
    schemas.add(NODEGROUP_SCHEMA);
    schemas.add(LEAF_SCHEMA);

    NodeSchemaManager manager = NodeSchemaManager.getInstance();
    manager.init(schemas.toArray(new NodeSchema[0]), true);
    NetworkTopology newCluster =
        new NetworkTopologyImpl(manager, mockedShuffleOperation);
    Node[] nodeList = new Node[] {
        createDatanode("1.1.1.1", "/r1/ng1"),
        createDatanode("2.2.2.2", "/r1/ng1"),
        createDatanode("3.3.3.3", "/r1/ng2"),
        createDatanode("4.4.4.4", "/r2/ng1"),
    };
    for (Node node: nodeList) {
      newCluster.add(node);
    }
    Node outScopeNode1 = createDatanode("5.5.5.5", "/r2/ng2");
    Node outScopeNode2 = createDatanode("6.6.6.6", "/r2/ng2");
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(nodeList[0], null));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(null, nodeList[0]));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(outScopeNode1, nodeList[0]));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(nodeList[0], outScopeNode1));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(outScopeNode1, outScopeNode2));

    assertEquals(0, newCluster.getDistanceCost(null, null));
    assertEquals(0, newCluster.getDistanceCost(nodeList[0], nodeList[0]));
    assertEquals(2, newCluster.getDistanceCost(nodeList[0], nodeList[1]));
    assertEquals(4, newCluster.getDistanceCost(nodeList[0], nodeList[2]));
    assertEquals(6, newCluster.getDistanceCost(nodeList[0], nodeList[3]));

    // network topology with customized cost
    schemas.clear();
    schemas.add(new NodeSchema.Builder()
        .setType(NodeSchema.LayerType.ROOT).setCost(5).build());
    schemas.add(new NodeSchema.Builder()
        .setType(NodeSchema.LayerType.INNER_NODE).setCost(3).build());
    schemas.add(new NodeSchema.Builder()
        .setType(NodeSchema.LayerType.INNER_NODE).setCost(1).build());
    schemas.add(new NodeSchema.Builder()
        .setType(NodeSchema.LayerType.LEAF_NODE).build());
    manager = NodeSchemaManager.getInstance();
    manager.init(schemas.toArray(new NodeSchema[0]), true);
    newCluster = new NetworkTopologyImpl(manager, mockedShuffleOperation);
    for (Node node: nodeList) {
      newCluster.add(node);
    }
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(nodeList[0], null));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(null, nodeList[0]));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(outScopeNode1, nodeList[0]));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(nodeList[0], outScopeNode1));
    assertEquals(Integer.MAX_VALUE,
        newCluster.getDistanceCost(outScopeNode1, outScopeNode2));

    assertEquals(0, newCluster.getDistanceCost(null, null));
    assertEquals(0, newCluster.getDistanceCost(nodeList[0], nodeList[0]));
    assertEquals(2, newCluster.getDistanceCost(nodeList[0], nodeList[1]));
    assertEquals(8, newCluster.getDistanceCost(nodeList[0], nodeList[2]));
    assertEquals(18, newCluster.getDistanceCost(nodeList[0], nodeList[3]));
  }

  @ParameterizedTest
  @MethodSource("topologies")
  void testSortByDistanceCost(NodeSchema[] schemas, Node[] nodeArray) {
    initNetworkTopology(schemas, nodeArray);
    Node[][] nodes = {
        {},
        {dataNodes[0]},
        {dataNodes[dataNodes.length - 1]},
        {dataNodes[random.nextInt(dataNodes.length)]},
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)]
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        },
        {dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
            dataNodes[random.nextInt(dataNodes.length)],
        }};
    Node[] readers = {null, dataNodes[0], dataNodes[dataNodes.length - 1],
        dataNodes[random.nextInt(dataNodes.length)],
        dataNodes[random.nextInt(dataNodes.length)],
        dataNodes[random.nextInt(dataNodes.length)]
    };
    for (Node reader : readers) {
      for (Node[] nodeList : nodes) {
        int length = nodeList.length;
        while (length > 0) {
          List<? extends Node> ret = cluster.sortByDistanceCost(reader,
              Arrays.asList(nodeList), length);
          assertEquals(length, ret.size());
          for (int i = 0; i < ret.size(); i++) {
            if ((i + 1) < ret.size()) {
              int cost1 = cluster.getDistanceCost(reader, ret.get(i));
              int cost2 = cluster.getDistanceCost(reader, ret.get(i + 1));
              assertTrue(cost1 == Integer.MAX_VALUE || cost1 <= cost2,
                  "reader:" + (reader != null ?
                      reader.getNetworkFullPath() : "null") +
                      ",node1:" + ret.get(i).getNetworkFullPath() +
                      ",node2:" + ret.get(i + 1).getNetworkFullPath() +
                      ",cost1:" + cost1 + ",cost2:" + cost2);
            }
          }
          length--;
        }
      }
    }

    // sort all nodes
    List<Node> nodeList = Arrays.asList(dataNodes.clone());
    for (Node reader : readers) {
      int length = nodeList.size();
      while (length >= 0) {
        List<? extends Node> sortedNodeList =
            cluster.sortByDistanceCost(reader, nodeList, length);
        assertEquals(length, sortedNodeList.size());
        for (int i = 0; i < sortedNodeList.size(); i++) {
          if ((i + 1) < sortedNodeList.size()) {
            int cost1 = cluster.getDistanceCost(reader, sortedNodeList.get(i));
            int cost2 = cluster.getDistanceCost(
                reader, sortedNodeList.get(i + 1));
            // node can be removed when called in testConcurrentAccess
            assertTrue(cost1 == Integer.MAX_VALUE || cost1 <= cost2,
                "reader:" + (reader != null ?
                    reader.getNetworkFullPath() : "null") +
                    ",node1:" + sortedNodeList.get(i).getNetworkFullPath() +
                    ",node2:" + sortedNodeList.get(i + 1).getNetworkFullPath() +
                    ",cost1:" + cost1 + ",cost2:" + cost2);
          }
        }
        length--;
      }
    }
  }

  @ParameterizedTest
  @MethodSource("topologies")
  void testSortByDistanceCostNullReader(NodeSchema[] schemas,
      Node[] nodeArray) {
    // GIVEN
    // various cluster topologies with null reader
    initNetworkTopology(schemas, nodeArray);
    List<Node> nodeList = Arrays.asList(dataNodes.clone());
    final Node reader = null;
    NetworkTopology spyCluster = spy(cluster);
    int length = nodeList.size();
    while (length > 0) {
      // WHEN
      List<? extends Node> ret = spyCluster.sortByDistanceCost(reader,
          nodeList, length);
      // THEN
      // no actual distance cost calculated
      // only shuffle input node list with given length limit
      verify(mockedShuffleOperation).accept(any());
      verify(spyCluster, never()).getDistanceCost(any(), any());
      assertEquals(length, ret.size());
      assertThat(nodeList).containsAll(ret);
      reset(mockedShuffleOperation);
      length--;
    }
  }

  @Test
  void testSingleNodeRackWithAffinityNode() {
    // network topology with default cost
    List<NodeSchema> schemas = new ArrayList<>();
    schemas.add(ROOT_SCHEMA);
    schemas.add(RACK_SCHEMA);
    schemas.add(LEAF_SCHEMA);

    NodeSchemaManager manager = NodeSchemaManager.getInstance();
    manager.init(schemas.toArray(new NodeSchema[0]), true);
    NetworkTopology newCluster =
        new NetworkTopologyImpl(manager, mockedShuffleOperation);
    Node node = createDatanode("1.1.1.1", "/r1");
    newCluster.add(node);
    Node chosenNode =
        newCluster.chooseRandom(NetConstants.ROOT, null, null, node, 0);
    assertNull(chosenNode);
    chosenNode = newCluster.chooseRandom(NetConstants.ROOT, null,
        Arrays.asList(node), node, 0);
    assertNull(chosenNode);
    chosenNode = newCluster.chooseRandom(NetConstants.ROOT,
        Arrays.asList(node.getNetworkFullPath()), Arrays.asList(node), node, 0);
    assertNull(chosenNode);
  }

  @Test
  void testUpdateNode() {
    List<NodeSchema> schemas = new ArrayList<>();
    schemas.add(ROOT_SCHEMA);
    schemas.add(DATACENTER_SCHEMA);
    schemas.add(RACK_SCHEMA);
    schemas.add(LEAF_SCHEMA);

    NodeSchemaManager manager = NodeSchemaManager.getInstance();
    manager.init(schemas.toArray(new NodeSchema[0]), true);
    NetworkTopology newCluster =
            new NetworkTopologyImpl(manager, mockedShuffleOperation);
    Node node = createDatanode("1.1.1.1", "/d1/r1");
    newCluster.add(node);
    assertTrue(newCluster.contains(node));

    // update
    Node newNode = createDatanode("1.1.1.2", "/d1/r1");
    assertFalse(newCluster.contains(newNode));
    newCluster.update(node, newNode);
    assertFalse(newCluster.contains(node));
    assertTrue(newCluster.contains(newNode));

    // update a non-existing node
    Node nodeExisting = createDatanode("1.1.1.3", "/d1/r1");
    Node newNode2 = createDatanode("1.1.1.4", "/d1/r1");
    assertFalse(newCluster.contains(nodeExisting));
    assertFalse(newCluster.contains(newNode2));

    newCluster.update(nodeExisting, newNode2);
    assertFalse(newCluster.contains(nodeExisting));
    assertTrue(newCluster.contains(newNode2));

    // old node is null
    Node newNode3 = createDatanode("1.1.1.5", "/d1/r1");
    assertFalse(newCluster.contains(newNode3));
    newCluster.update(null, newNode3);
    assertTrue(newCluster.contains(newNode3));
  }

  @Test
  void testIsAncestor() {
    NodeImpl r1 = new NodeImpl("r1", "/", NODE_COST_DEFAULT);
    NodeImpl r12 = new NodeImpl("r12", "/", NODE_COST_DEFAULT);
    NodeImpl dc = new NodeImpl("dc", "/r12", NODE_COST_DEFAULT);
    assertFalse(r1.isAncestor(dc));
    assertFalse(r1.isAncestor("/r12/dc2"));
    assertFalse(dc.isDescendant(r1));
    assertFalse(dc.isDescendant("/r1"));

    assertTrue(r12.isAncestor(dc));
    assertTrue(r12.isAncestor("/r12/dc2"));
    assertTrue(dc.isDescendant(r12));
    assertTrue(dc.isDescendant("/r12"));
  }

  @Test
  void testGetLeafOnLeafParent() {
    InnerNodeImpl root = new InnerNodeImpl("", "", null, 0, 0);
    InnerNodeImpl r12 = new InnerNodeImpl("r12", "/", root, 1, 0);
    InnerNodeImpl dc = new InnerNodeImpl("dc", "/r12", r12, 2, 0);
    NodeImpl n1 = new NodeImpl("n1", "/r12/dc", dc, 2, 0);
    dc.add(n1);

    List<String> excludedScope = new ArrayList<>();
    excludedScope.add("/r1");
    assertFalse(n1.isDescendant("/r1"));
    assertEquals(n1, dc.getLeaf(0, excludedScope, null, 0));
  }

  private static Node createDatanode(String name, String path) {
    return new NodeImpl(name, path, NetConstants.NODE_COST_DEFAULT);
  }

  /**
   * This picks a large number of nodes at random in order to ensure coverage.
   *
   * @param numNodes the number of nodes
   * @param excludedScope the excluded scope
   * @param excludedNodes the excluded node list
   * @param ancestorGen the chosen node cannot share the same ancestor at
   *                    this generation with excludedNodes
   * @return the frequency that nodes were chosen
   */
  private Map<Node, Integer> pickNodesAtRandom(int numNodes,
      String excludedScope, Collection<Node> excludedNodes, int ancestorGen) {
    Map<Node, Integer> frequency = new HashMap<>();
    for (Node dnd : dataNodes) {
      frequency.put(dnd, 0);
    }
    for (int j = 0; j < numNodes; j++) {
      Node node = cluster.chooseRandom(excludedScope, excludedNodes,
          ancestorGen);
      if (node != null) {
        frequency.put(node, frequency.get(node) + 1);
      }
    }
    LOG.info("Result:{}", frequency);
    return frequency;
  }

  /**
   * This picks a large number of nodes at random in order to ensure coverage.
   *
   * @param numNodes the number of nodes
   * @param excludedScope the excluded scope
   * @param excludedNodes the excluded node list
   * @param affinityNode the chosen node should share the same ancestor at
   *                     generation "ancestorGen" with this node
   * @param ancestorGen  the chosen node cannot share the same ancestor at
   *                     this generation with excludedNodes
   * @return the frequency that nodes were chosen
   */
  private Map<Node, Integer> pickNodesAtRandom(int numNodes,
      String excludedScope, Collection<Node> excludedNodes, Node affinityNode,
      int ancestorGen) {
    Map<Node, Integer> frequency = new HashMap<>();
    for (Node dnd : dataNodes) {
      frequency.put(dnd, 0);
    }

    List<String> pathList = new ArrayList<>();
    pathList.add(excludedScope.substring(1));
    for (int j = 0; j < numNodes; j++) {

      Node node = cluster.chooseRandom("", pathList, excludedNodes,
          affinityNode, ancestorGen);
      if (node != null) {
        frequency.put(node, frequency.get(node) + 1);
      }
    }
    LOG.info("Result:{}", frequency);
    return frequency;
  }

  /**
   * This picks a large amount of nodes sequentially.
   *
   * @param numNodes the number of nodes
   * @param excludedScopes the excluded scopes, should not start with "~"
   * @param excludedNodes the excluded node list
   * @param affinityNode the chosen node should share the same ancestor at
   *                     generation "ancestorGen" with this node
   * @param ancestorGen  the chosen node cannot share the same ancestor at
   *                     this generation with excludedNodes
   * @return the frequency that nodes were chosen
   */
  private Map<Node, Integer> pickNodes(int numNodes,
      List<String> excludedScopes, Collection<Node> excludedNodes,
      Node affinityNode, int ancestorGen) {
    Map<Node, Integer> frequency = new HashMap<>();
    for (Node dnd : dataNodes) {
      frequency.put(dnd, 0);
    }
    excludedNodes = excludedNodes == null ? null :
        excludedNodes.stream().distinct().collect(Collectors.toList());
    for (int j = 0; j < numNodes; j++) {
      Node node = cluster.getNode(j, null, excludedScopes, excludedNodes,
          affinityNode, ancestorGen);
      if (node != null) {
        frequency.put(node, frequency.get(node) + 1);
      }
    }

    LOG.info("Result:{}", frequency);
    return frequency;
  }
}
