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

package org.apache.hadoop.hdds.scm.security;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX;
import static org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType.CERT_ROTATE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Root CA Rotation Handler for ratis SCM statemachine.
 */
public class RootCARotationHandlerImpl implements RootCARotationHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(RootCARotationHandlerImpl.class);

  private final StorageContainerManager scm;
  private final SCMCertificateClient scmCertClient;
  private final SecurityConfig secConfig;
  private Set<String> newScmCertIdSet = new HashSet<>();
  private final String newSubCAPath;
  private final RootCARotationManager rotationManager;
  private AtomicReference<String> newSubCACertId = new AtomicReference();
  private AtomicReference<String> newRootCACertId = new AtomicReference();

  /**
   * Constructs RootCARotationHandlerImpl with the specified arguments.
   *
   * @param scm the storage container manager
   */
  public RootCARotationHandlerImpl(StorageContainerManager scm,
      RootCARotationManager manager) {
    this.scm = scm;
    this.rotationManager = manager;
    this.scmCertClient = (SCMCertificateClient) scm.getScmCertificateClient();
    this.secConfig = scmCertClient.getSecurityConfig();

    this.newSubCAPath = secConfig.getLocation(
        scmCertClient.getComponentName()).toString()
        + HDDS_NEW_KEY_CERT_DIR_NAME_SUFFIX;
  }

  @Override
  public void rotationPrepare(String rootCertId)
      throws IOException {
    LOG.info("Received rotation prepare command of root certificate {}",
        rootCertId);
    if (rotationManager.shouldSkipRootCert(rootCertId)) {
      return;
    }

    newRootCACertId.set(rootCertId);
    newScmCertIdSet.clear();
    newSubCACertId.set(null);
    rotationManager.scheduleSubCaRotationPrepareTask(rootCertId);
  }

  @Override
  public void rotationPrepareAck(String rootCertId,
      String scmCertId, String scmId) throws IOException {
    LOG.info("Received rotation prepare ack of root certificate {} from scm {}",
        rootCertId, scmId);

    // Only leader count the acks
    if (rotationManager.isRunning()) {
      if (rotationManager.shouldSkipRootCert(rootCertId)) {
        return;
      }
      if (rootCertId.equals(newRootCACertId.get())) {
        newScmCertIdSet.add(scmCertId);
      }
    }
  }

  @Override
  public void rotationCommit(String rootCertId)
      throws IOException {
    LOG.info("Received rotation commit command of root certificate {}",
        rootCertId);
    if (rotationManager.shouldSkipRootCert(rootCertId)) {
      return;
    }

    // switch sub CA key and certs directory on disk
    File currentSubCaDir = new File(secConfig.getLocation(
        scmCertClient.getComponentName()).toString());
    File backupSubCaDir = new File(secConfig.getLocation(
        scmCertClient.getComponentName() +
            HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX).toString());
    File newSubCaDir = new File(newSubCAPath);

    try {
      // move current -> backup
      Files.move(currentSubCaDir.toPath(), backupSubCaDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      LOG.error("Failed to move {} to {}", currentSubCaDir, backupSubCaDir, e);
      String message = "Terminate SCM, encounter IO exception(" +
          e.getMessage() + ") when move " + currentSubCaDir + " to " +
          backupSubCaDir;
      scm.shutDown(message);
    }

    try {
      // move new -> current
      Files.move(newSubCaDir.toPath(), currentSubCaDir.toPath(),
          StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      LOG.error("Failed to move {} to {}", newSubCaDir, currentSubCaDir, e);
      String message = "Terminate SCM, encounter IO exception(" +
          e.getMessage() + ") when move " + newSubCaDir + " to " +
          currentSubCaDir;
      scm.shutDown(message);
    }

    try {
      String certId = newSubCACertId.get();
      LOG.info("Persistent new scm certificate {}", certId);
      scm.getScmStorageConfig().setScmCertSerialId(certId);
      scm.getScmStorageConfig().persistCurrentState();
    } catch (IOException e) {
      LOG.error("Failed to persist new SCM certificate ID", e);
      String message = "Terminate SCM, encounter IO exception(" +
          e.getMessage() + ") when persist new SCM certificate ID";
      scm.shutDown(message);
    }
  }

  @Override
  public void rotationCommitted(String rootCertId)
      throws IOException {
    LOG.info("Received rotation committed command of root certificate {}",
        rootCertId);
    if (rotationManager.shouldSkipRootCert(rootCertId)) {
      return;
    }

    // turn on new root CA certificate and sub CA certificate
    scmCertClient.reloadKeyAndCertificate(newSubCACertId.get());

    // cleanup backup directory
    File backupSubCaDir = new File(secConfig.getLocation(
        scmCertClient.getComponentName() +
            HDDS_BACKUP_KEY_CERT_DIR_NAME_SUFFIX).toString());
    try {
      FileUtils.deleteDirectory(backupSubCaDir);
    } catch (IOException e) {
      LOG.error("Failed to delete backup dir {}", backupSubCaDir, e);
    }

    // reset state
    newSubCACertId.set(null);
  }

  @Override
  public int rotationPrepareAcks() {
    return newScmCertIdSet.size();
  }

  @Override
  public void resetRotationPrepareAcks() {
    newScmCertIdSet.clear();
  }

  @Override
  public void setSubCACertId(String subCACertId) {
    newSubCACertId.set(subCACertId);
    LOG.info("Scm sub CA new certificate is {}", subCACertId);
  }

  /**
   * Builder for RootCARotationHandlerImpl.
   */
  public static class Builder {
    private StorageContainerManager scm;
    private SCMRatisServer ratisServer;
    private RootCARotationManager rootCARotationManager;

    public Builder setRatisServer(
        final SCMRatisServer scmRatisServer) {
      this.ratisServer = scmRatisServer;
      return this;
    }

    public Builder setStorageContainerManager(
        final StorageContainerManager storageContainerManager) {
      scm = storageContainerManager;
      return this;
    }

    public Builder setRootCARotationManager(
        final RootCARotationManager manager) {
      rootCARotationManager = manager;
      return this;
    }

    public RootCARotationHandler build() {
      final RootCARotationHandler impl =
          new RootCARotationHandlerImpl(scm, rootCARotationManager);

      return ratisServer.getProxyHandler(CERT_ROTATE,
          RootCARotationHandler.class, impl);
    }
  }
}
