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
package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil.isSameSchemaVersion;

/**
 * Class to hold version info for container data and metadata.
 * - SchemaVersion: metadata schema version
 * - ChunkLayOutVersion: data layout version
 */
public class ContainerTestVersionInfo {
  private static final String[] SCHEMA_VERSIONS = new String[] {
      null,
      OzoneConsts.SCHEMA_V1,
      OzoneConsts.SCHEMA_V2,
      OzoneConsts.SCHEMA_V3,
  };

  private final String schemaVersion;
  private final ContainerLayoutVersion layout;

  public ContainerTestVersionInfo(String schemaVersion,
      ContainerLayoutVersion layout) {
    this.schemaVersion = schemaVersion;
    this.layout = layout;
  }

  private static List<ContainerTestVersionInfo> layoutList = new ArrayList<>();
  static {
    for (ContainerLayoutVersion ch : ContainerLayoutVersion.getAllVersions()) {
      for (String sch : SCHEMA_VERSIONS) {
        layoutList.add(new ContainerTestVersionInfo(sch, ch));
      }
    }
  }

  public String getSchemaVersion() {
    return this.schemaVersion;
  }

  public ContainerLayoutVersion getLayout() {
    return this.layout;
  }

  public static Iterable<Object[]> versionParameters() {
    return layoutList.stream().map(each -> new Object[] {each})
        .collect(toList());
  }

  /**
   * This method is created to support the parameterized data during
   * migration to Junit5.
   * @return Stream of ContainerTestVersionInfo objects.
   */
  public static Stream<Object> versionParametersStream() {
    return layoutList.stream().map(each -> new Object[] {each});
  }

  @Override
  public String toString() {
    return "schema=" + schemaVersion + ", layout=" + layout;
  }

  public static List<ContainerTestVersionInfo> getLayoutList() {
    return layoutList;
  }
  public static void setTestSchemaVersion(String schemaVersion,
      OzoneConfiguration conf) {
    if (isSameSchemaVersion(schemaVersion, OzoneConsts.SCHEMA_V3)) {
      ContainerTestUtils.enableSchemaV3(conf);
    } else {
      ContainerTestUtils.disableSchemaV3(conf);
    }
  }
}
