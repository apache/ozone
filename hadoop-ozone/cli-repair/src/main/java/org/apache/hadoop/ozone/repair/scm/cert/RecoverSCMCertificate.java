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

package org.apache.hadoop.ozone.repair.scm.cert;

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.VALID_SCM_CERTS;
import static org.apache.hadoop.hdds.security.x509.certificate.client.DefaultCertificateClient.CERT_FILE_NAME_FORMAT;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.removeTrailingSlashIfNeeded;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.debug.DBDefinitionFactory;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.repair.RepairTool;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

/**
 * In case of accidental deletion of SCM certificates from local storage,
 * this tool restores the certs that are persisted into the SCM DB.
 * Note that this will only work if the SCM has persisted certs in its RocksDB
 * and  private keys of the SCM are intact.
 */
@CommandLine.Command(
    name = "recover",
    description = "Recover Deleted SCM Certificate from RocksDB."  +
        " SCM should be stopped for this tool.")
public class RecoverSCMCertificate extends RepairTool {
  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "SCM DB Path")
  private String dbPath;

  @Nonnull
  @Override
  protected Component serviceToBeOffline() {
    return Component.SCM;
  }

  @Override
  public void execute() throws Exception {
    dbPath = removeTrailingSlashIfNeeded(dbPath);
    String tableName = VALID_SCM_CERTS.getName();
    DBDefinition dbDefinition =
        DBDefinitionFactory.getDefinition(Paths.get(dbPath), new OzoneConfiguration());
    if (dbDefinition == null) {
      throw new Exception("Error: Incorrect DB Path");
    }
    DBColumnFamilyDefinition columnFamilyDefinition =
        getDbColumnFamilyDefinition(tableName, dbDefinition);

    try {
      List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(dbPath);
      final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
      byte[] tableNameBytes = tableName.getBytes(StandardCharsets.UTF_8);
      ColumnFamilyHandle cfHandle = null;
      try (ManagedRocksDB db = ManagedRocksDB.openReadOnly(dbPath, cfDescList,
          cfHandleList)) {
        cfHandle = getColumnFamilyHandle(cfHandleList, tableNameBytes);
        SecurityConfig securityConfig = new SecurityConfig(getOzoneConf());

        Map<BigInteger, X509Certificate> allCerts = getAllCerts(columnFamilyDefinition, cfHandle, db);
        info("All Certs in DB : %s", allCerts.keySet());
        String hostName = InetAddress.getLocalHost().getHostName();
        info("Host: %s", hostName);

        X509Certificate subCertificate = getSubCertificate(allCerts, hostName);
        X509Certificate rootCertificate = getRootCertificate(allCerts);

        info("Sub cert serialID for this host: %s", subCertificate.getSerialNumber());
        info("Root cert serialID: %s", rootCertificate.getSerialNumber());

        boolean isRootCA = false;

        String caPrincipal = rootCertificate.getSubjectDN().getName();
        if (caPrincipal.contains(hostName)) {
          isRootCA = true;
        }
        storeCerts(subCertificate, rootCertificate, isRootCA, securityConfig);
      }
    } catch (RocksDBException | CertificateException exception) {
      error("Failed to recover scm cert");
    }
  }

  private static ColumnFamilyHandle getColumnFamilyHandle(
      List<ColumnFamilyHandle> cfHandleList, byte[] tableNameBytes) throws Exception {
    ColumnFamilyHandle cfHandle = null;
    for (ColumnFamilyHandle cf : cfHandleList) {
      if (Arrays.equals(cf.getName(), tableNameBytes)) {
        cfHandle = cf;
        break;
      }
    }
    if (cfHandle == null) {
      throw new Exception("Error: VALID_SCM_CERTS table not found in DB");
    }
    return cfHandle;
  }

  private static X509Certificate getRootCertificate(
      Map<BigInteger, X509Certificate> allCerts) throws Exception {
    Optional<X509Certificate> cert = allCerts.values().stream().filter(
        c -> c.getSubjectDN().getName()
            .contains(OzoneConsts.SCM_ROOT_CA_PREFIX)).findFirst();
    if (!cert.isPresent()) {
      throw new Exception("Root CA Cert not found in the DB for this host, Certs in the DB : " + allCerts.keySet());
    }
    return cert.get();
  }

  private static X509Certificate getSubCertificate(
      Map<BigInteger, X509Certificate> allCerts, String hostName) throws Exception {
    Optional<X509Certificate> cert = allCerts.values().stream().filter(
        c -> c.getSubjectDN().getName()
            .contains(OzoneConsts.SCM_SUB_CA_PREFIX) && c.getSubjectDN()
            .getName().contains(hostName)).findFirst();
    if (!cert.isPresent()) {
      throw new Exception("Sub CA Cert not found in the DB for this host, Certs in the DB : " + allCerts.keySet());
    }
    return cert.get();
  }

  private static Map<BigInteger, X509Certificate> getAllCerts(
      DBColumnFamilyDefinition columnFamilyDefinition,
      ColumnFamilyHandle cfHandle, ManagedRocksDB db) throws IOException, RocksDBException {
    Map<BigInteger, X509Certificate> allCerts = new HashMap<>();
    ManagedRocksIterator rocksIterator = ManagedRocksIterator.managed(db.get().newIterator(cfHandle));
    rocksIterator.get().seekToFirst();
    while (rocksIterator.get().isValid()) {
      BigInteger id = (BigInteger) columnFamilyDefinition.getKeyCodec()
          .fromPersistedFormat(rocksIterator.get().key());
      X509Certificate certificate =
          (X509Certificate) columnFamilyDefinition.getValueCodec()
              .fromPersistedFormat(rocksIterator.get().value());
      allCerts.put(id, certificate);
      rocksIterator.get().next();
    }
    return allCerts;
  }

  private static DBColumnFamilyDefinition getDbColumnFamilyDefinition(
      String tableName, DBDefinition dbDefinition) throws Exception {
    DBColumnFamilyDefinition columnFamilyDefinition =
        dbDefinition.getColumnFamily(tableName);
    if (columnFamilyDefinition == null) {
      throw new Exception(
          "Error: VALID_SCM_CERTS table no found in Definition");
    }
    return columnFamilyDefinition;
  }

  private void storeCerts(X509Certificate scmCertificate,
      X509Certificate rootCertificate, boolean isRootCA, SecurityConfig securityConfig)
      throws CertificateException, IOException {
    CertificateCodec certCodec =
        new CertificateCodec(securityConfig, SCMCertificateClient.COMPONENT_NAME);

    CertPath certPath = addRootCertInPath(scmCertificate, rootCertificate);
    CertPath rootCertPath = getRootCertPath(rootCertificate);
    String encodedCert = CertificateCodec.getPEMEncodedString(certPath);
    String certName = String.format(CERT_FILE_NAME_FORMAT,
        CAType.NONE.getFileNamePrefix() + scmCertificate.getSerialNumber());
    writeCertificate(certCodec, certName, encodedCert);

    String rootCertName = String.format(CERT_FILE_NAME_FORMAT,
        CAType.SUBORDINATE.getFileNamePrefix() + rootCertificate.getSerialNumber());
    String encodedRootCert = CertificateCodec.getPEMEncodedString(rootCertPath);
    writeCertificate(certCodec, rootCertName, encodedRootCert);

    writeCertificate(certCodec, securityConfig.getCertificateFileName(), encodedCert);

    if (isRootCA) {
      CertificateCodec rootCertCodec =
          new CertificateCodec(securityConfig, OzoneConsts.SCM_ROOT_CA_COMPONENT_NAME);
      writeCertificate(rootCertCodec, securityConfig.getCertificateFileName(), encodedRootCert);
    }
  }

  private void writeCertificate(CertificateCodec codec, String name, String encodedCert) throws IOException {
    info("Writing cert %s to %s", name, codec.getLocation());
    if (!isDryRun()) {
      codec.writeCertificate(name, encodedCert);
    }
  }

  public CertPath addRootCertInPath(X509Certificate scmCert,
      X509Certificate rootCert) throws CertificateException {
    ArrayList<X509Certificate> updatedList = new ArrayList<>();
    updatedList.add(scmCert);
    updatedList.add(rootCert);
    CertificateFactory certFactory =
        CertificateCodec.getCertFactory();
    return certFactory.generateCertPath(updatedList);
  }

  public CertPath getRootCertPath(X509Certificate rootCert)
      throws CertificateException {
    ArrayList<X509Certificate> updatedList = new ArrayList<>();
    updatedList.add(rootCert);
    CertificateFactory factory = CertificateCodec.getCertFactory();
    return factory.generateCertPath(updatedList);
  }
}
