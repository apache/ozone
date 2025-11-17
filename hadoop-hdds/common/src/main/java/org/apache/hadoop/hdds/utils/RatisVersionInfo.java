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

package org.apache.hadoop.hdds.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.hadoop.util.ThreadUtil;
import org.slf4j.LoggerFactory;

/**
 * This class returns build information about Ratis projects.
 */
public class RatisVersionInfo {

  private static final String RATIS_VERSION_PROPERTIES =
      "ratis-version.properties";

  private final Properties info = new Properties();

  public RatisVersionInfo() {
    try (InputStream is = ThreadUtil.getResourceAsStream(
        getClass().getClassLoader(),
        RATIS_VERSION_PROPERTIES)) {
      info.load(is);
    } catch (IOException ex) {
      LoggerFactory.getLogger(getClass()).warn("Could not read " +
          RATIS_VERSION_PROPERTIES, ex);
    }
  }

  public String getVersion() {
    return info.getProperty("version", "Unknown");
  }

  public String getRevision() {
    return info.getProperty("revision", "Unknown");
  }

  public String getBuildVersion() {
    return getVersion() +
        " from " + getRevision();
  }
}
