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

import static org.apache.hadoop.hdds.scm.net.NetConstants.PATH_SEPARATOR;
import static org.apache.hadoop.hdds.scm.net.NetConstants.PATH_SEPARATOR_STR;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thread safe class that implements InnerNode interface.
 */
public class InnerNodeImpl extends NodeImpl implements InnerNode {
  // LOGGER
  private static final Logger LOG = LoggerFactory.getLogger(InnerNodeImpl.class);

  public static final Factory FACTORY = new Factory();
  // a map of node's network name to Node for quick search and keep
  // the insert order
  private HashMap<String, Node> childrenMap = new LinkedHashMap<String, Node>();
  // number of descendant leaves under this node
  private int numOfLeaves;

  /**
   * Construct an InnerNode from its name, network location, parent, level and
   * its cost.
   **/
  protected InnerNodeImpl(String name, String location, InnerNode parent,
      int level, int cost) {
    super(name, location, parent, level, cost);
  }

  /**
   * Construct an InnerNode from its name, network location, level, cost,
   * childrenMap and number of leaves. This constructor is used as part of
   * protobuf deserialization.
   */
  protected InnerNodeImpl(String name, String location, int level, int cost,
                          HashMap<String, Node> childrenMap, int numOfLeaves) {
    super(name, location, null, level, cost);
    this.childrenMap = childrenMap;
    this.numOfLeaves = numOfLeaves;
  }

  /**
   * InnerNodeImpl Builder to help construct an InnerNodeImpl object from
   * protobuf objects.
   */
  public static class Builder {
    private String name;
    private String location;
    private int cost;
    private int level;
    private HashMap<String, Node> childrenMap = new LinkedHashMap<>();
    private int numOfLeaves;

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setLocation(String location) {
      this.location = location;
      return this;
    }

    public Builder setCost(int cost) {
      this.cost = cost;
      return this;
    }

    public Builder setLevel(int level) {
      this.level = level;
      return this;
    }

    public Builder setChildrenMap(
        List<HddsProtos.ChildrenMap> childrenMapList) {
      HashMap<String, Node> newChildrenMap = new LinkedHashMap<>();
      for (HddsProtos.ChildrenMap childrenMapProto :
          childrenMapList) {
        String networkName = childrenMapProto.hasNetworkName() ?
            childrenMapProto.getNetworkName() : null;
        Node node = childrenMapProto.hasNetworkNode() ?
            Node.fromProtobuf(childrenMapProto.getNetworkNode()) : null;
        newChildrenMap.put(networkName, node);
      }
      this.childrenMap = newChildrenMap;
      return this;
    }

    public Builder setNumOfLeaves(int numOfLeaves) {
      this.numOfLeaves = numOfLeaves;
      return this;
    }

    public InnerNodeImpl build() {
      return new InnerNodeImpl(name, location, level, cost, childrenMap,
          numOfLeaves);
    }
  }

  /** @return the number of children this node has */
  private int getNumOfChildren() {
    return childrenMap.size();
  }

  /** @return its leaf nodes number */
  @Override
  public int getNumOfLeaves() {
    return numOfLeaves;
  }

  /** @return a map of node's network name to Node. */
  public HashMap<String, Node> getChildrenMap() {
    return childrenMap;
  }

  /**
   * @return number of its all nodes at level <i>level</i>. Here level is a
   * relative level. If level is 1, means node itself. If level is 2, means its
   * direct children, and so on.
   **/
  @Override
  public int getNumOfNodes(int level) {
    Preconditions.checkArgument(level > 0);
    int count = 0;
    if (level == 1) {
      count += 1;
    } else if (level == 2) {
      count += getNumOfChildren();
    } else {
      for (Node node: childrenMap.values()) {
        if (node instanceof InnerNode) {
          count += ((InnerNode)node).getNumOfNodes(level - 1);
        } else {
          throw new RuntimeException("Cannot support Level:" + level +
              " on this node " + this.toString());
        }
      }
    }
    return count;
  }

  /**
   * @return all nodes at level <i>level</i>. Here level is a relative level.
   * If level is 1, means node itself. If level is 2, means its direct
   * children, and so on.
   **/
  @Override
  public List<Node> getNodes(int level) {
    Preconditions.checkArgument(level > 0);
    List<Node> result = new ArrayList<>();
    if (level == 1) {
      result.add(this);
    } else if (level == 2) {
      result.addAll(childrenMap.values());
    } else {
      for (Node node: childrenMap.values()) {
        if (node instanceof InnerNode) {
          result.addAll(((InnerNode)node).getNodes(level - 1));
        } else {
          throw new RuntimeException("Cannot support Level:" + level +
              " on this node " + this.toString());
        }
      }
    }
    return result;
  }

  /**
   * Judge if this node is the parent of a leave node <i>n</i>.
   * @return true if this node is the parent of <i>n</i>
   */
  private boolean isLeafParent() {
    if (childrenMap.isEmpty()) {
      return true;
    }
    Node child = childrenMap.values().iterator().next();
    return !(child instanceof InnerNode);
  }

  /**
   * Judge if this node is the parent of node <i>node</i>.
   * @param node a node
   * @return true if this node is the parent of <i>n</i>
   */
  private boolean isParent(Node node) {
    return node.getNetworkLocation().equals(this.getNetworkFullPath());
  }

  /**
   * Add node <i>node</i> to the subtree of this node.
   * @param node node to be added
   * @return true if the node is added, false if is only updated
   */
  @Override
  public boolean add(Node node) {
    if (!isAncestor(node)) {
      throw new IllegalArgumentException(node.getNetworkName()
          + ", which is located at " + node.getNetworkLocation()
          + ", is not a descendant of " + this.getNetworkFullPath());
    }
    if (isParent(node)) {
      // this node is the parent, then add it directly
      node.setParent(this);
      node.setLevel(this.getLevel() + 1);
      Node current = childrenMap.put(node.getNetworkName(), node);
      if (current != null) {
        return false;
      }
    } else {
      // find the next level ancestor node
      String ancestorName = getNextLevelAncestorName(node);
      InnerNode childNode = (InnerNode)childrenMap.get(ancestorName);
      if (childNode == null) {
        // create a new InnerNode for this ancestor node
        childNode = createChildNode(ancestorName);
        childrenMap.put(childNode.getNetworkName(), childNode);
      }
      // add node to the subtree of the next ancestor node
      if (!childNode.add(node)) {
        return false;
      }
    }
    numOfLeaves++;
    return true;
  }

  /**
   * Remove node <i>node</i> from the subtree of this node.
   * @param node node to be deleted
   */
  @Override
  public void remove(Node node) {
    if (!isAncestor(node)) {
      throw new IllegalArgumentException(node.getNetworkName()
          + ", which is located at " + node.getNetworkLocation()
          + ", is not a descendant of " + this.getNetworkFullPath());
    }
    if (isParent(node)) {
      // this node is the parent, remove it directly
      if (childrenMap.containsKey(node.getNetworkName())) {
        childrenMap.remove(node.getNetworkName());
        node.setParent(null);
      } else {
        throw new RuntimeException("Should not come to here. Node:" +
            node.getNetworkFullPath() + ", Parent:" +
            this.getNetworkFullPath());
      }
    } else {
      // find the next ancestor node
      String ancestorName = getNextLevelAncestorName(node);
      InnerNodeImpl childNode = (InnerNodeImpl)childrenMap.get(ancestorName);
      Objects.requireNonNull(childNode, "InnerNode is deleted before leaf");
      // remove node from the parent node
      childNode.remove(node);
      // if the parent node has no children, remove the parent node too
      if (childNode.getNumOfChildren() == 0) {
        childrenMap.remove(ancestorName);
      }
    }
    numOfLeaves--;
  }

  /**
   * Given a node's string representation, return a reference to the node.
   * Node can be leaf node or inner node.
   *
   * @param loc string location of a node. If loc starts with "/", it's a
   *            absolute path, otherwise a relative path. Following examples
   *            are all accepted,
   *            <pre>
   *            {@code
   *            1.  /dc1/rm1/rack1          -> an inner node
   *            2.  /dc1/rm1/rack1/node1    -> a leaf node
   *            3.  rack1/node1             -> a relative path to this node
   *            }
   *            </pre>
   * @return null if the node is not found
   */
  @Override
  public Node getNode(String loc) {
    if (loc == null) {
      return null;
    }

    String fullPath = this.getNetworkFullPath();
    if (loc.equalsIgnoreCase(fullPath)) {
      return this;
    }

    // remove current node's location from loc when it's a absolute path
    if (fullPath.equals(NetConstants.PATH_SEPARATOR_STR)) {
      // current node is ROOT
      if (loc.startsWith(PATH_SEPARATOR_STR)) {
        loc = loc.substring(1);
      }
    } else if (loc.startsWith(fullPath)) {
      loc = loc.substring(fullPath.length());
      // skip the separator "/"
      loc = loc.substring(1);
    }

    String[] path = loc.split(PATH_SEPARATOR_STR, 2);
    Node child = childrenMap.get(path[0]);
    if (child == null) {
      return null;
    }
    if (path.length == 1) {
      return child;
    }
    if (child instanceof InnerNode) {
      return ((InnerNode)child).getNode(path[1]);
    } else {
      return null;
    }
  }

  /**
   * get <i>leafIndex</i> leaf of this subtree.
   *
   * @param leafIndex an indexed leaf of the node
   * @return the leaf node corresponding to the given index.
   */
  @Override
  public Node getLeaf(int leafIndex) {
    Preconditions.checkArgument(leafIndex >= 0);
    // children are leaves
    if (isLeafParent()) {
      // range check
      if (leafIndex >= getNumOfChildren()) {
        return null;
      }
      return getChildNode(leafIndex);
    } else {
      for (Node node : childrenMap.values()) {
        InnerNodeImpl child = (InnerNodeImpl)node;
        int leafCount = child.getNumOfLeaves();
        if (leafIndex < leafCount) {
          return child.getLeaf(leafIndex);
        } else {
          leafIndex -= leafCount;
        }
      }
      return null;
    }
  }

  /**
   * Get <i>leafIndex</i> leaf of this subtree.
   *
   * @param leafIndex node's index, start from 0, skip the nodes in
   *                 excludedScope and excludedNodes with ancestorGen
   * @param excludedScopes the exclude scopes
   * @param excludedNodes nodes to be excluded from. If ancestorGen is not 0,
   *                      the chosen node will not share same ancestor with
   *                      those in excluded nodes at the specified generation
   * @param ancestorGen  apply to excludeNodes, when value is 0, then no same
   *                    ancestor enforcement on excludedNodes
   * @return the leaf node corresponding to the given index.
   * Example:
   *
   *                                /  --- root
   *                              /  \
   *                             /    \
   *                            /      \
   *                           /        \
   *                         dc1         dc2
   *                        / \         / \
   *                       /   \       /   \
   *                      /     \     /     \
   *                    rack1 rack2  rack1  rack2
   *                   / \     / \  / \     / \
   *                 n1  n2  n3 n4 n5  n6  n7 n8
   *
   *   Input:
   *   leafIndex = 2
   *   excludedScope = /dc2/rack2
   *   excludedNodes = {/dc1/rack1/n1}
   *   ancestorGen = 1
   *
   *   Output:
   *   node /dc1/rack2/n5
   *
   *   Explanation:
   *   Since excludedNodes is n1 and ancestorGen is 1, it means nodes under
   *   /root/dc1/rack1 are excluded. Given leafIndex start from 0, LeafIndex 2
   *   means picking the 3th available node, which is n5.
   *
   */
  @Override
  public Node getLeaf(int leafIndex, List<String> excludedScopes,
      Collection<Node> excludedNodes, int ancestorGen) {
    Preconditions.checkArgument(leafIndex >= 0 && ancestorGen >= 0);
    // come to leaf parent layer
    if (isLeafParent()) {
      return getLeafOnLeafParent(leafIndex, excludedScopes, excludedNodes);
    }

    int maxLevel = NodeSchemaManager.getInstance().getMaxLevel();
    // this node's children, it's generation as the ancestor of the leaf node
    int currentGen = maxLevel - this.getLevel() - 1;
    // build an ancestor(children) to exclude node count map
    Map<Node, Integer> countMap =
        getAncestorCountMap(excludedNodes, ancestorGen, currentGen);
    // nodes covered by excluded scope
    Map<String, Integer> excludedNodeCount =
        getExcludedScopeNodeCount(excludedScopes);

    for (Node child : childrenMap.values()) {
      int leafCount = child.getNumOfLeaves();
      // skip nodes covered by excluded scopes
      for (Map.Entry<String, Integer> entry: excludedNodeCount.entrySet()) {
        if (entry.getKey().startsWith(child.getNetworkFullPath())) {
          leafCount -= entry.getValue();
        }
      }
      // skip nodes covered by excluded nodes and ancestorGen
      Integer count = countMap.get(child);
      if (count != null) {
        leafCount -= count;
      }
      if (leafIndex < leafCount) {
        return ((InnerNode)child).getLeaf(leafIndex, excludedScopes,
            excludedNodes, ancestorGen);
      } else {
        leafIndex -= leafCount;
      }
    }
    return null;
  }

  @Override
  public HddsProtos.NetworkNode toProtobuf(
      int clientVersion) {

    HddsProtos.InnerNode.Builder innerNode =
        HddsProtos.InnerNode.newBuilder()
            .setNumOfLeaves(numOfLeaves)
            .setNodeTopology(
                NodeImpl.toProtobuf(getNetworkName(), getNetworkLocation(),
                    getLevel(), getCost()));

    if (childrenMap != null && !childrenMap.isEmpty()) {
      for (Map.Entry<String, Node> entry : childrenMap.entrySet()) {
        if (entry.getValue() != null) {
          HddsProtos.ChildrenMap childrenMapProto =
              HddsProtos.ChildrenMap.newBuilder()
                  .setNetworkName(entry.getKey())
                  .setNetworkNode(entry.getValue().toProtobuf(clientVersion))
                  .build();
          innerNode.addChildrenMap(childrenMapProto);
        }
      }
    }
    innerNode.build();

    HddsProtos.NetworkNode networkNode =
        HddsProtos.NetworkNode.newBuilder()
            .setInnerNode(innerNode).build();

    return networkNode;
  }

  public static InnerNode fromProtobuf(HddsProtos.InnerNode innerNode) {
    InnerNodeImpl.Builder builder = new InnerNodeImpl.Builder();

    if (innerNode.hasNodeTopology()) {
      HddsProtos.NodeTopology nodeTopology = innerNode.getNodeTopology();

      if (nodeTopology.hasName()) {
        builder.setName(nodeTopology.getName());
      }
      if (nodeTopology.hasLocation()) {
        builder.setLocation(nodeTopology.getLocation());
      }
      if (nodeTopology.hasLevel()) {
        builder.setLevel(nodeTopology.getLevel());
      }
      if (nodeTopology.hasCost()) {
        builder.setCost(nodeTopology.getCost());
      }
    }

    if (!innerNode.getChildrenMapList().isEmpty()) {
      builder.setChildrenMap(innerNode.getChildrenMapList());
    }
    if (innerNode.hasNumOfLeaves()) {
      builder.setNumOfLeaves(innerNode.getNumOfLeaves());
    }

    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InnerNodeImpl innerNode = (InnerNodeImpl) o;
    return this.getNetworkName().equals(innerNode.getNetworkName()) &&
        this.getNetworkLocation().equals(innerNode.getNetworkLocation()) &&
        this.getLevel() == innerNode.getLevel() &&
        this.getCost() == innerNode.getCost() &&
        this.numOfLeaves == innerNode.numOfLeaves &&
        this.childrenMap.size() == innerNode.childrenMap.size() &&
        this.childrenMap.equals(innerNode.childrenMap);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Get a ancestor to its excluded node count map.
   *
   * @param nodes a collection of leaf nodes to exclude
   * @param genToExclude  the ancestor generation to exclude
   * @param genToReturn  the ancestor generation to return the count map
   * @return the map.
   * example:
   *
   *                *  --- root
   *              /    \
   *             *      *   -- genToReturn =2
   *            / \    / \
   *          *   *   *   *  -- genToExclude = 1
   *         /\  /\  /\  /\
   *       *  * * * * * * *  -- nodes
   */
  private Map<Node, Integer> getAncestorCountMap(Collection<Node> nodes,
      int genToExclude, int genToReturn) {
    Preconditions.checkState(genToExclude >= 0);
    Preconditions.checkState(genToReturn >= 0);

    if (nodes == null || nodes.isEmpty()) {
      return Collections.emptyMap();
    }
    // with the recursive call, genToReturn can be smaller than genToExclude
    if (genToReturn < genToExclude) {
      genToExclude = genToReturn;
    }
    // ancestorToExclude to ancestorToReturn map
    HashMap<Node, Node> ancestorMap = new HashMap<>();
    for (Node node: nodes) {
      Node ancestorToExclude = node.getAncestor(genToExclude);
      Node ancestorToReturn = node.getAncestor(genToReturn);
      if (ancestorToExclude == null || ancestorToReturn == null) {
        LOG.warn("Ancestor not found, node: {}"
            + ", generation to exclude: {}"
            + ", generation to return: {}", node.getNetworkFullPath(),
                genToExclude, genToReturn);
        continue;
      }
      ancestorMap.put(ancestorToExclude, ancestorToReturn);
    }
    // ancestorToReturn to exclude node count map
    HashMap<Node, Integer> countMap = new HashMap<>();
    for (Map.Entry<Node, Node> entry : ancestorMap.entrySet()) {
      countMap.compute(entry.getValue(),
          (key, n) -> (n == null ? 0 : n) + entry.getKey().getNumOfLeaves());
    }

    return countMap;
  }

  /**
   *  Get the node with leafIndex, considering skip nodes in excludedScope
   *  and in excludeNodes list.
   */
  private Node getLeafOnLeafParent(int leafIndex, List<String> excludedScopes,
      Collection<Node> excludedNodes) {
    Preconditions.checkArgument(isLeafParent() && leafIndex >= 0);
    if (leafIndex >= getNumOfChildren()) {
      return null;
    }
    for (Node node : childrenMap.values()) {
      if (excludedNodes != null && excludedNodes.contains(node)) {
        continue;
      }
      if (excludedScopes != null && !excludedScopes.isEmpty()) {
        if (excludedScopes.stream().anyMatch(node::isDescendant)) {
          continue;
        }
      }
      if (leafIndex == 0) {
        return node;
      }
      leafIndex--;
    }
    return null;
  }

  /**
   *  Return child's name of this node which is an ancestor of node <i>n</i>.
   */
  private String getNextLevelAncestorName(Node n) {
    int parentPathLen = this.getNetworkFullPath().length();
    String name = n.getNetworkLocation().substring(parentPathLen);
    if (name.charAt(0) == PATH_SEPARATOR) {
      name = name.substring(1);
    }
    int index = name.indexOf(PATH_SEPARATOR);
    if (index != -1) {
      name = name.substring(0, index);
    }
    return name;
  }

  /**
   * Creates a child node to be added to the list of children.
   * @param name The name of the child node
   * @return A new inner node
   * @see InnerNodeImpl(String, String, InnerNode, int)
   */
  private InnerNodeImpl createChildNode(String name) {
    int childLevel = this.getLevel() + 1;
    int cost = NodeSchemaManager.getInstance().getCost(childLevel);
    return new InnerNodeImpl(name, this.getNetworkFullPath(), this, childLevel,
        cost);
  }

  /** Get node with index <i>index</i>. */
  private Node getChildNode(int index) {
    Iterator iterator = childrenMap.values().iterator();
    Node node = null;
    while (index >= 0 && iterator.hasNext()) {
      node = (Node)iterator.next();
      index--;
    }
    return node;
  }

  /** Get how many leaf nodes are covered by the excludedScopes(no overlap). */
  private Map<String, Integer> getExcludedScopeNodeCount(
      List<String> excludedScopes) {
    HashMap<String, Integer> nodeCounts = new HashMap<>();
    if (excludedScopes == null || excludedScopes.isEmpty()) {
      return nodeCounts;
    }

    for (String scope: excludedScopes) {
      Node excludedScopeNode = getNode(scope);
      nodeCounts.put(scope, excludedScopeNode == null ? 0 :
          excludedScopeNode.getNumOfLeaves());
    }
    return nodeCounts;
  }

  protected static class Factory implements InnerNode.Factory<InnerNodeImpl> {
    protected Factory() { }

    @Override
    public InnerNodeImpl newInnerNode(String name, String location,
        InnerNode parent, int level, int cost) {
      return new InnerNodeImpl(name, location, parent, level, cost);
    }
  }
}
