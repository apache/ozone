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

package org.apache.hadoop.ozone.debug.kdiag;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Validates Ozone and Hadoop RPC authorization configuration.
 */
public class AuthorizationProbe implements DiagnosticProbe {

  @Override
  public String name() {
    return "Authorization Configuration";
  }

  @Override
  public boolean run() {
    System.out.println("-- Authorization Configuration --");
    OzoneConfiguration conf = new OzoneConfiguration();

    print(conf, "ozone.acl.enabled");
    print(conf, "ozone.acl.authorizer.class");
    print(conf, "hadoop.security.authorization");
    print(conf, "ozone.om.security.client.protocol.acl");

    print(conf, "hdds.security.client.datanode.container.protocol.acl");
    print(conf, "hdds.security.client.scm.container.protocol.acl");
    print(conf, "hdds.security.client.scm.block.protocol.acl");
    print(conf, "hdds.security.client.scm.certificate.protocol.acl");
    return true;
  }

  private void print(OzoneConfiguration conf, String key) {
    String value = conf.get(key);
    System.out.println(key + " = "
        + (value == null ? "(unset)" : value));
  }
}
