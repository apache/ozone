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

package org.apache.hadoop.ozone.conf;

import static org.apache.hadoop.hdds.conf.ConfigTag.CLIENT;
import static org.apache.hadoop.hdds.conf.ConfigTag.OM;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * Config for OM Client.
 */
@ConfigGroup(prefix = "ozone.om.client")
public class OMClientConfig {

  @Config(key = "ozone.om.client.rpc.timeout",
      defaultValue = "15m",
      type = ConfigType.TIME,
      tags = {OZONE, OM, CLIENT},
      timeUnit = TimeUnit.MILLISECONDS,
      description = "RpcClient timeout on waiting for the response from " +
          "OzoneManager. The default value is set to 15 minutes. " +
          "If ipc.client.ping is set to true and this rpc-timeout " +
          "is greater than the value of ipc.ping.interval, the effective " +
          "value of the rpc-timeout is rounded up to multiple of " +
          "ipc.ping.interval."
  )
  private long rpcTimeOut = 15 * 60 * 1000;

  @Config(key = "ozone.om.client.trash.core.pool.size",
      defaultValue = "5",
      type = ConfigType.INT,
      tags = {OZONE, OM, CLIENT},
      description = "Total number of threads in pool for the Trash Emptier")
  private int trashEmptierPoolSize = 5;

  public int getTrashEmptierPoolSize() {
    return trashEmptierPoolSize;
  }

  public void setTrashEmptierPoolSize(int trashEmptierPoolSize) {
    this.trashEmptierPoolSize = trashEmptierPoolSize;
  }

  public long getRpcTimeOut() {
    return rpcTimeOut;
  }

  public void setRpcTimeOut(long timeOut) {
    // As at the end this value should not exceed MAX_VALUE, as underlying
    // Rpc layer SocketTimeout parameter is int.
    if (rpcTimeOut > Integer.MAX_VALUE) {
      this.rpcTimeOut = Integer.MAX_VALUE;
    }
    this.rpcTimeOut = timeOut;
  }
}
