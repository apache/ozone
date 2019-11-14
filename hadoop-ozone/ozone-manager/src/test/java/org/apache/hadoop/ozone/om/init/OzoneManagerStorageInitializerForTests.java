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
package org.apache.hadoop.ozone.om.init;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class OzoneManagerStorageInitializerForTests {
  private OzoneManagerStorageInitializerForTests(){
    // disable instantiation
  }

  /**
   * Runs the standard storage initialization based on the given configuration.
   * @see OzoneManagerStorageInitializer#run(OzoneConfiguration)
   * @param configuration the curent configuration of the cluster.
   * @throws IOException if communication with other components fails
   * @throws AuthenticationException if security is enabled, and login fails.
   */
  public static boolean run(OzoneConfiguration configuration)
      throws IOException, AuthenticationException {
    return OzoneManagerStorageInitializer.run(configuration);
  }

  /**
   * Initialize the OM VERSION file with the given data. The place of it is
   * specified by the configuration.
   * In case of enabled security certificate data is gathered from the SCM,
   * however there is no login happening for the OM user so that security
   * context issues inside one JVM with UGI is worked around here, and there has
   * to be an already initialize UGI in the JVM to succeed.
   *
   * @param configuration the current configuration
   * @param clusterId the clusterID to set in the VERSION file, specify null to
   *                  generate one randomly
   * @param scmId the SCM ID to set in the VERSION file, specify null to
   *              generate one randomly
   * @param omId the OM ID to set in the VERSION file, specify null to
   *             generate one randomly
   */
  public static void run(OzoneConfiguration configuration, String clusterId,
      String scmId, String omId) throws IOException {
    OMStorage storage = createBaseOMStorage(configuration, clusterId, scmId, omId);
    if (OzoneSecurityUtil.isSecurityEnabled(configuration)){
      SecurityInitializer.initializeSecurity(configuration, storage);
    }
    storage.initialize();
  }

  /**
   *
   * @param configuration the current configuration
   * @param clusterId the clusterID to set in the VERSION file, specify null to
   *                  generate one randomly
   * @param scmId the SCM ID to set in the VERSION file, specify null to
   *              generate one randomly
   * @param omId the OM ID to set in the VERSION file, specify null to
   *             generate one randomly
   * @param certSerialId the certificate serial ID to set in the VERSION file,
   *                     specify null to generate one randomly.
   * @throws IOException
   */
  public static void run(OzoneConfiguration configuration, String clusterId,
      String scmId, String omId, String certSerialId) throws IOException {
    OMStorage storage = createBaseOMStorage(configuration, clusterId, scmId, omId);
    storage.setOmCertSerialId(checkCertId(certSerialId));
    storage.initialize();
  }

  private static OMStorage createBaseOMStorage(OzoneConfiguration configuration,
      String clusterId, String scmId, String omId) throws IOException {
    OMStorage storage = new OMStorage(configuration);
    storage.setClusterId(checkId(clusterId));
    storage.setScmId(checkId(scmId));
    storage.setOmId(checkId(omId));
    return storage;
  }

  private static String checkId(String id){
    if (id == null){
      return UUID.randomUUID().toString();
    }
    return id;
  }

  private static String checkCertId(String id){
    if (id == null){
      return Integer.toString(new Random().nextInt(10000));
    }
    return id;
  }
}
