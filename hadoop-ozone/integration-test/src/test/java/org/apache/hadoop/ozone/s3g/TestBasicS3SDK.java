/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.s3g;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;

/**
 * Tests the S3 compatibility using AWS Java SDK for non-ratis cluster.
 */
public class TestBasicS3SDK extends TestBasicS3SDKAbstract {

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    startCluster(conf);
  }

  @AfterAll
  public static void shutdown() throws IOException {
    shutdownCluster();
  }
}
