/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.security;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import org.junit.Rule;
import org.junit.Before;
import org.junit.After;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;

/**
 * Base class for testing OM delegation token request.
 */
@SuppressWarnings("visibilitymodifier")
public class TestOMDelegationTokenRequest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  protected OzoneManager ozoneManager;
  protected OMMetrics omMetrics;
  protected OMMetadataManager omMetadataManager;
  protected ConfigurationSource conf;

  // Just setting OzoneManagerDoubleBuffer which does nothing.
  protected OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
      ((response, transactionIndex) -> {
        return null;
      });

  @Before
  public void setup() throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);

    conf = new OzoneConfiguration();
    ((OzoneConfiguration) conf)
        .set(OZONE_OM_DB_DIRS, folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl((OzoneConfiguration) conf);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
  }

  @After
  public void stop() {
    Mockito.framework().clearInlineMocks();
  }
}
