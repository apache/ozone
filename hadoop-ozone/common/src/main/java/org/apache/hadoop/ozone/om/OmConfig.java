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

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.PostConstruct;

/**
 * Ozone Manager configuration.
 */
@ConfigGroup(prefix = "ozone.om")
public class OmConfig {

  @Config(
      key = "server.list.max.size",
      defaultValue = "1000",
      description = "Configuration property to configure the max server side response size for list calls on om.",
      tags = { ConfigTag.OM, ConfigTag.OZONE }
  )
  private long maxListSize;

  public long getMaxListSize() {
    return maxListSize;
  }

  public void setMaxListSize(long newValue) {
    maxListSize = newValue;
    validate();
  }

  @PostConstruct
  public void validate() {
    if (maxListSize <= 0) {
      maxListSize = Defaults.SERVER_LIST_MAX_SIZE;
    }
  }

  /**
   * String keys for tests and grep.
   */
  public static final class Keys {
    public static final String SERVER_LIST_MAX_SIZE = "ozone.om.server.list.max.size";
  }

  /**
   * Default values for tests.
   */
  static final class Defaults {
    public static final long SERVER_LIST_MAX_SIZE = 1000;
  }

}
