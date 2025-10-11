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

import org.apache.hadoop.hdds.scm.net.NodeSchema.LayerType;
import org.apache.hadoop.ozone.util.StringWithByteString;

/**
 * Class to hold network topology related constants and configurations.
 */
public final class NetConstants {
  public static final char PATH_SEPARATOR = '/';
  /** Path separator as a string. */
  public static final String PATH_SEPARATOR_STR = "/";
  public static final String SCOPE_REVERSE_STR = "~";
  /** string representation of root. */
  public static final String ROOT = "";
  public static final StringWithByteString BYTE_STRING_ROOT = StringWithByteString.valueOf(ROOT);
  public static final int INNER_NODE_COST_DEFAULT = 1;
  public static final int NODE_COST_DEFAULT = 0;
  public static final int ANCESTOR_GENERATION_DEFAULT = 0;
  public static final int ROOT_LEVEL = 1;
  public static final String DEFAULT_RACK = "/default-rack";
  public static final StringWithByteString BYTE_STRING_DEFAULT_RACK = StringWithByteString.valueOf(DEFAULT_RACK);
  public static final String DEFAULT_NODEGROUP = "/default-nodegroup";
  public static final String DEFAULT_DATACENTER = "/default-datacenter";
  public static final String DEFAULT_REGION = "/default-dataregion";

  // Build-in network topology node schema
  public static final NodeSchema ROOT_SCHEMA =
      new NodeSchema.Builder().setType(LayerType.ROOT).build();

  public static final NodeSchema REGION_SCHEMA =
      new NodeSchema.Builder().setType(LayerType.INNER_NODE)
          .setDefaultName(DEFAULT_REGION).build();

  public static final NodeSchema DATACENTER_SCHEMA =
      new NodeSchema.Builder().setType(LayerType.INNER_NODE)
          .setDefaultName(DEFAULT_DATACENTER).build();

  public static final NodeSchema RACK_SCHEMA =
      new NodeSchema.Builder().setType(LayerType.INNER_NODE)
          .setDefaultName(DEFAULT_RACK).build();

  public static final NodeSchema NODEGROUP_SCHEMA =
      new NodeSchema.Builder().setType(LayerType.INNER_NODE)
          .setDefaultName(DEFAULT_NODEGROUP).build();

  public static final NodeSchema LEAF_SCHEMA =
      new NodeSchema.Builder().setType(LayerType.LEAF_NODE).build();

  private NetConstants() {
    // Prevent instantiation
  }
}
