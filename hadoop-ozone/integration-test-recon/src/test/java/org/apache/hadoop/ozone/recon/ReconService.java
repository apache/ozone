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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_TASK_SAFEMODE_WAIT_THRESHOLD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR;
import static org.apache.ozone.test.GenericTestUtils.PortAllocator.localhostWithFreePort;

import java.io.File;
import java.util.Objects;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ratis.util.Preconditions;

/** Recon for {@link MiniOzoneCluster}. */
public class ReconService implements MiniOzoneCluster.Service {

  private static final String[] NO_ARGS = new String[0];

  private final String httpAddress;
  private final String datanodeAddress;

  private ReconServer reconServer;

  public ReconService(OzoneConfiguration conf) {
    httpAddress = localhostWithFreePort();
    datanodeAddress = localhostWithFreePort();
    setReconAddress(conf);
  }

  @Override
  public void start(OzoneConfiguration conf) {
    Preconditions.assertNull(reconServer, "Recon already started");

    ConfigurationProvider.resetConfiguration();
    ConfigurationProvider.setConfiguration(conf);
    configureRecon(conf);

    reconServer = new ReconServer();
    reconServer.execute(NO_ARGS);
  }

  @Override
  public void stop() {
    final ReconServer instance = reconServer;
    Preconditions.assertNotNull(instance, "Recon not running");
    instance.stop();
    instance.join();
    reconServer = null;
  }

  @Override
  public String toString() {
    final ReconServer instance = reconServer;
    return instance != null
        ? "Recon(http=" + instance.getHttpServer().getHttpAddress()
            + ", https=" + instance.getHttpServer().getHttpsAddress() + ")"
        : "Recon";
  }

  ReconServer getReconServer() {
    return reconServer;
  }

  private void configureRecon(OzoneConfiguration conf) {
    String metadataDir = Objects.requireNonNull(conf.get(OZONE_METADATA_DIRS), OZONE_METADATA_DIRS + " must be set");
    File dir = new File(metadataDir, "recon");
    conf.set(OZONE_RECON_DB_DIR, dir.getAbsolutePath());
    conf.set(OZONE_RECON_OM_SNAPSHOT_DB_DIR, dir.getAbsolutePath());
    conf.set(OZONE_RECON_SCM_DB_DIR, dir.getAbsolutePath());

    ReconSqlDbConfig dbConfig = conf.getObject(ReconSqlDbConfig.class);
    dbConfig.setJdbcUrl("jdbc:derby:" + dir.getAbsolutePath()
        + "/ozone_recon_derby.db");
    conf.setFromObject(dbConfig);

    conf.set(OZONE_RECON_TASK_SAFEMODE_WAIT_THRESHOLD, "10s");

    setReconAddress(conf);
  }

  private void setReconAddress(OzoneConfiguration conf) {
    conf.set(OZONE_RECON_ADDRESS_KEY, datanodeAddress);
    conf.set(OZONE_RECON_DATANODE_ADDRESS_KEY, datanodeAddress);
    conf.set(OZONE_RECON_HTTP_ADDRESS_KEY, httpAddress);
  }
}
