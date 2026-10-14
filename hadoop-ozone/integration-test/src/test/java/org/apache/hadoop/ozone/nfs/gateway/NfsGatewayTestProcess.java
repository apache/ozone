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

package org.apache.hadoop.ozone.nfs.gateway;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

/** Runs a gateway in a separate JVM for process-crash integration tests. */
public final class NfsGatewayTestProcess {
  private NfsGatewayTestProcess() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Expected an Ozone configuration file");
    }
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.addResource(new Path(args[0]));
    UserGroupInformation.setConfiguration(configuration);
    try (NfsGatewayServer server = NfsGatewayDaemon.start(configuration)) {
      System.out.println("NFS_GATEWAY_READY " + server.getPort());
      System.out.flush();
      server.awaitTermination();
    }
  }
}
