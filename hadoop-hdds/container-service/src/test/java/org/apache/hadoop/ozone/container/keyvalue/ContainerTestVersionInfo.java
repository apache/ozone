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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil.isSameSchemaVersion;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Class to hold version info for container data and metadata.
 * - SchemaVersion: metadata schema version
 * - ChunkLayOutVersion: data layout version
 */
public class ContainerTestVersionInfo {

  private static List<ContainerTestVersionInfo> layoutList = new ArrayList<>();

  private static final String[] SCHEMA_VERSIONS = new String[] {
      null,
      OzoneConsts.SCHEMA_V1,
      OzoneConsts.SCHEMA_V2,
      OzoneConsts.SCHEMA_V3,
  };

  static {
    for (ContainerLayoutVersion ch : ContainerLayoutVersion.getAllVersions()) {
      for (String sch : SCHEMA_VERSIONS) {
        layoutList.add(new ContainerTestVersionInfo(sch, ch));
      }
    }
  }

  private final String schemaVersion;
  private final ContainerLayoutVersion layout;

  /**
   * Composite annotation for tests parameterized with {@link ContainerTestVersionInfo}.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @ParameterizedTest
  @MethodSource("org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo#getLayoutList")
  public @interface ContainerTest {
    // composite annotation
  }

  public ContainerTestVersionInfo(String schemaVersion,
      ContainerLayoutVersion layout) {
    this.schemaVersion = schemaVersion;
    this.layout = layout;
  }

  public String getSchemaVersion() {
    return this.schemaVersion;
  }

  public ContainerLayoutVersion getLayout() {
    return this.layout;
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
