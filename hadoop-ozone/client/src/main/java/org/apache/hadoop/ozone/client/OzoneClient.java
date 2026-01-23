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

package org.apache.hadoop.ozone.client;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.ratis.util.UncheckedAutoCloseable;

/**
 * OzoneClient connects to Ozone Cluster and
 * perform basic operations.
 */
public class OzoneClient implements Closeable {

  /*
   * OzoneClient connects to Ozone Cluster and
   * perform basic operations.
   *
   * +-------------+     +---+   +-------------------------------------+
   * | OzoneClient | --> | C |   | Object Store                        |
   * |_____________|     | l |   |  +-------------------------------+  |
   *                     | i |   |  | Volume(s)                     |  |
   *                     | e |   |  |   +------------------------+  |  |
   *                     | n |   |  |   | Bucket(s)              |  |  |
   *                     | t |   |  |   |   +------------------+ |  |  |
   *                     |   |   |  |   |   | Key -> Value (s) | |  |  |
   *                     | P |-->|  |   |   |                  | |  |  |
   *                     | r |   |  |   |   |__________________| |  |  |
   *                     | o |   |  |   |                        |  |  |
   *                     | t |   |  |   |________________________|  |  |
   *                     | o |   |  |                               |  |
   *                     | c |   |  |_______________________________|  |
   *                     | o |   |                                     |
   *                     | l |   |_____________________________________|
   *                     |___|
   * Example:
   * ObjectStore store = client.getObjectStore();
   * store.createVolume(“volume one”, VolumeArgs);
   * volume.setQuota(“10 GB”);
   * OzoneVolume volume = store.getVolume(“volume one”);
   * volume.createBucket(“bucket one”, BucketArgs);
   * bucket.setVersioning(true);
   * OzoneOutputStream os = bucket.createKey(“key one”, 1024);
   * os.write(byte[]);
   * os.close();
   * OzoneInputStream is = bucket.readKey(“key one”);
   * is.read();
   * is.close();
   * bucket.deleteKey(“key one”);
   * volume.deleteBucket(“bucket one”);
   * store.deleteVolume(“volume one”);
   * client.close();
   */

  private final ClientProtocol proxy;
  private final ObjectStore objectStore;
  private  ConfigurationSource conf;
  private final UncheckedAutoCloseable leakTracker = OzoneClientFactory.track(this);

  /**
   * Creates a new OzoneClient object, generally constructed
   * using {@link OzoneClientFactory}.
   * @param conf Configuration object
   * @param proxy ClientProtocol proxy instance
   */
  public OzoneClient(ConfigurationSource conf, ClientProtocol proxy) {
    this.proxy = proxy;
    this.objectStore = new ObjectStore(conf, this.proxy);
    this.conf = conf;
  }

  @VisibleForTesting
  protected OzoneClient(ObjectStore objectStore,
                        ClientProtocol clientProtocol) {
    this.objectStore = objectStore;
    this.proxy = clientProtocol;
    // For the unit test
    this.conf = new OzoneConfiguration();
  }

  /**
   * Returns the object store associated with the Ozone Cluster.
   * @return ObjectStore
   */
  public ObjectStore getObjectStore() {
    return objectStore;
  }

  /**
   * Returns the configuration of client.
   * @return ConfigurationSource
   */
  public ConfigurationSource getConfiguration() {
    return conf;
  }

  /**
   * Closes the client and all the underlying resources.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    try {
      proxy.close();
    } finally {
      leakTracker.close();
    }
  }

  /**
   * Return the ClientProtocol associated with the Ozone Cluster.
   * @return ClientProtocol
   */
  public ClientProtocol getProxy() {
    return proxy;
  }
}
