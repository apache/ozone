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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the class that provides the instance of the DB Store used by Recon to
 * store its container - key data.
 */
public class TestReconDBProvider {

  @TempDir
  private Path temporaryFolder;

  private Injector injector;

  @BeforeEach
  public void setUp() throws IOException {
    injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        File dbDir = temporaryFolder.toFile();
        OzoneConfiguration configuration = new OzoneConfiguration();
        configuration.set(OZONE_RECON_DB_DIR, dbDir.getAbsolutePath());
        bind(OzoneConfiguration.class).toInstance(configuration);
        bind(ReconDBProvider.class).in(Singleton.class);
      }
    });
  }

  @Test
  public void testGet() throws Exception {
    ReconDBProvider reconDBProvider = injector.getInstance(
        ReconDBProvider.class);
    assertNotNull(reconDBProvider.getDbStore());
  }

}
