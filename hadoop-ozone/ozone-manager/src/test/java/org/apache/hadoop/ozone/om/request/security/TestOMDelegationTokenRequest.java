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

package org.apache.hadoop.ozone.om.request.security;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Base class for testing OM delegation token request.
 */
@SuppressWarnings("visibilitymodifier")
public class TestOMDelegationTokenRequest {

  @TempDir
  private Path folder;

  protected OzoneManager ozoneManager;
  protected OMMetadataManager omMetadataManager;
  protected ConfigurationSource conf;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);

    conf = new OzoneConfiguration();
    ((OzoneConfiguration) conf)
        .set(OZONE_OM_DB_DIRS, folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl((OzoneConfiguration) conf,
        ozoneManager);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
  }

  @AfterEach
  public void stop() {
    framework().clearInlineMocks();
  }
}
