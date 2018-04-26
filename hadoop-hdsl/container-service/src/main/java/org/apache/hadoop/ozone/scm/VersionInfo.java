/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.scm;

/**
 * This is a class that tracks versions of SCM.
 */
public final class VersionInfo {

  // We will just be normal and use positive counting numbers for versions.
  private final static VersionInfo[] VERSION_INFOS =
      {new VersionInfo("First version of SCM", 1)};


  public static final String DESCRIPTION_KEY = "Description";
  private final String description;
  private final int version;

  /**
   * Never created outside this class.
   *
   * @param description -- description
   * @param version     -- version number
   */
  private VersionInfo(String description, int version) {
    this.description = description;
    this.version = version;
  }

  /**
   * Returns all versions.
   *
   * @return Version info array.
   */
  public static VersionInfo[] getAllVersions() {
    return VERSION_INFOS.clone();
  }

  /**
   * Returns the latest version.
   *
   * @return versionInfo
   */
  public static VersionInfo getLatestVersion() {
    return VERSION_INFOS[VERSION_INFOS.length - 1];
  }

  /**
   * Return description.
   *
   * @return String
   */
  public String getDescription() {
    return description;
  }

  /**
   * Return the version.
   *
   * @return int.
   */
  public int getVersion() {
    return version;
  }

}
