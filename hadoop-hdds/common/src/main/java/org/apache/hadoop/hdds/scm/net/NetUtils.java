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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to facilitate network topology functions.
 */
public final class NetUtils {

  private static final Logger LOG = LoggerFactory.getLogger(NetUtils.class);

  private NetUtils() {
    // Prevent instantiation
  }

  /**
   * Normalize a path by stripping off any trailing.
   * {@link NetConstants#PATH_SEPARATOR}
   * @param path path to normalize.
   * @return the normalised path
   * If <i>path</i>is empty or null, then {@link NetConstants#ROOT} is returned
   */
  public static String normalize(String path) {
    if (path == null) {
      return NetConstants.ROOT;
    }

    int len = path.length();
    if (len == 0) {
      return NetConstants.ROOT;
    }

    if (path.charAt(0) != NetConstants.PATH_SEPARATOR) {
      throw new IllegalArgumentException(
          "Network Location path does not start with "
              + NetConstants.PATH_SEPARATOR_STR + ": " + path);
    }

    // Remove any trailing NetConstants.PATH_SEPARATOR
    return len == 1 ? path : StringUtils.removeEnd(path,
        NetConstants.PATH_SEPARATOR_STR);
  }

  /**
   *  Given a network topology location string, return its network topology
   *  depth, E.g. the depth of /dc1/rack1/ng1/node1 is 5.
   */
  public static int locationToDepth(String location) {
    String newLocation = normalize(location);
    return newLocation.equals(NetConstants.PATH_SEPARATOR_STR) ? 1 :
        newLocation.split(NetConstants.PATH_SEPARATOR_STR).length;
  }

  /**
   *  Remove node from mutableExcludedNodes if it's covered by excludedScope.
   *  Please noted that mutableExcludedNodes content might be changed after the
   *  function call.
   */
  public static void removeDuplicate(NetworkTopology topology,
      Collection<Node> mutableExcludedNodes, List<String> mutableExcludedScopes,
      int ancestorGen) {
    if (CollectionUtils.isEmpty(mutableExcludedNodes) ||
        CollectionUtils.isEmpty(mutableExcludedScopes) || topology == null) {
      return;
    }

    Iterator<Node> iterator = mutableExcludedNodes.iterator();
    while (iterator.hasNext() && (!mutableExcludedScopes.isEmpty())) {
      Node node = iterator.next();
      Node ancestor = topology.getAncestor(node, ancestorGen);
      if (ancestor == null) {
        LOG.warn("Fail to get ancestor generation {} of node :{}",
            ancestorGen, node);
        continue;
      }
      // excludedScope is child of ancestor
      mutableExcludedScopes.removeIf(ancestor::isAncestor);

      // ancestor is covered by excludedScope
      for (String scope : mutableExcludedScopes) {
        if (ancestor.isDescendant(scope)) {
          // remove exclude node if it's covered by excludedScope
          iterator.remove();
        }
      }
    }
  }

  /**
   * Get a ancestor list for nodes on generation <i>generation</i>.
   *
   * @param nodes a collection of leaf nodes
   * @param generation  the ancestor generation
   * @return the ancestor list. If no ancestor is found, then a empty list is
   * returned.
   */
  public static List<Node> getAncestorList(NetworkTopology topology,
      Collection<Node> nodes, int generation) {
    List<Node> ancestorList = new ArrayList<>();
    if (topology == null || CollectionUtils.isEmpty(nodes) ||
        generation == 0) {
      return ancestorList;
    }
    Iterator<Node> iterator = nodes.iterator();
    while (iterator.hasNext()) {
      Node node = iterator.next();
      Node ancestor = topology.getAncestor(node, generation);
      if (ancestor == null) {
        LOG.warn("Fail to get ancestor generation {} of node :{}",
            generation, node);
        continue;
      }
      if (!ancestorList.contains(ancestor)) {
        ancestorList.add(ancestor);
      }
    }
    return ancestorList;
  }

  /**
   * Ensure {@link NetConstants#PATH_SEPARATOR_STR} is added to the suffix of
   * the path.
   * @param path path to add suffix
   * @return the normalised path
   * If <i>path</i>is empty, then {@link NetConstants#PATH_SEPARATOR_STR} is
   * returned
   */
  public static String addSuffix(String path) {
    if (path == null) {
      return null;
    }
    if (!path.endsWith(NetConstants.PATH_SEPARATOR_STR)) {
      return path + NetConstants.PATH_SEPARATOR_STR;
    }
    return path;
  }
}
