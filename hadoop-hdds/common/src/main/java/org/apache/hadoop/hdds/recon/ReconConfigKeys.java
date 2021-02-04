/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.recon;

/**
 * This class contains constants for Recon related configuration keys used in
 * SCM & Datanode.
 */
public final class ReconConfigKeys {

  /**
   * Never constructed.
   */
  private ReconConfigKeys() {
  }

  public static final String RECON_SCM_CONFIG_PREFIX = "ozone.recon.scmconfig";

  public static final String OZONE_RECON_DATANODE_ADDRESS_KEY =
      "ozone.recon.datanode.address";
  public static final String OZONE_RECON_ADDRESS_KEY =
      "ozone.recon.address";
  public static final String OZONE_RECON_DATANODE_BIND_HOST_KEY =
      "ozone.recon.datanode.bind.host";
  public static final String OZONE_RECON_DATANODE_BIND_HOST_DEFAULT =
      "0.0.0.0";
  public static final int OZONE_RECON_DATANODE_PORT_DEFAULT = 9891;
  // Prometheus HTTP endpoint including port
  // ex: http://localhost:9090
  public static final String OZONE_RECON_PROMETHEUS_HTTP_ENDPOINT =
      "ozone.recon.prometheus.http.endpoint";
}
