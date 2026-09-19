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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertNotSame;

import javax.net.SocketFactory;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ClientCache;
import org.apache.hadoop.net.NetUtils;
import org.junit.jupiter.api.Test;

/** Tests {@link IdentitySocketFactory}. */
class TestIdentitySocketFactory {

  @Test
  void usesIdentityForHadoopClientCacheKeys() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    SocketFactory delegate = NetUtils.getDefaultSocketFactory(configuration);
    SocketFactory firstFactory = new IdentitySocketFactory(delegate);
    SocketFactory secondFactory = new IdentitySocketFactory(delegate);
    ClientCache clientCache = new ClientCache();
    Client firstClient = clientCache.getClient(configuration, firstFactory,
        ObjectWritable.class);
    Client secondClient = clientCache.getClient(configuration, secondFactory,
        ObjectWritable.class);

    try {
      assertNotSame(firstClient, secondClient);
    } finally {
      clientCache.stopClient(firstClient);
      clientCache.stopClient(secondClient);
    }
  }
}
