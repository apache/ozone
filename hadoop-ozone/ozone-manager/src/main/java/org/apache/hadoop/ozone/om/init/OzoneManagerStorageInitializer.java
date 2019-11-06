/**
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.init;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ha.OMHANodeDetails;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class should be the only visible part of this package.
 * The package encapsulates all the necessary logic to initialize Ozone
 * Manager's local data storage.
 *
 * To run the initialization, use
 * {@link OzoneManagerStorageInitializer#run(OzoneConfiguration)}.
 */
public abstract class OzoneManagerStorageInitializer {

  static final Logger LOG =
      LoggerFactory.getLogger(OzoneManager.class);

  /**
   * This method initialize the configured OzoneManager storage.
   *
   * If the storage is initialized already, but security just got enabled, then
   * it will update the storage directory with security related information.
   *
   * If a proper storage directory is there and it is initialized properly based
   * on the configuration, then no changes will be done by the code.
   *
   * @param conf the current {@link OzoneConfiguration}.
   * @return true if initialization was successful.
   * @throws IOException if the initialization failed due to IO issues.
   * @throws AuthenticationException if the security initialization failed due
   *                                  to a login failure.
   */
  public static boolean run(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    OMHANodeDetails.loadOMHAConfig(conf);
    try {
      getImplementation(conf).initialize(conf);
    } catch (AuthenticationException | IOException e){
      LOG.error("Could not initialize OM version file", e);
      throw e;
    }
    return true;
  }

  private static OzoneManagerStorageInitializer getImplementation(
      OzoneConfiguration conf) throws IOException, AuthenticationException {
    OMStorage omStorage = new OMStorage(conf);
    StorageState state = omStorage.getState();
    if (state != StorageState.INITIALIZED){
      if (OzoneSecurityUtil.isSecurityEnabled(conf)){
        return new FullSecureInitializer(omStorage).login(conf);
      }
      return new FullUnsecureInitializer(omStorage);
    } else {
      if (OzoneSecurityUtil.isSecurityEnabled(conf) &&
          omStorage.getOmCertSerialId() == null){
        return new SecurityInitializer(omStorage).login(conf);
      }
      System.out.println(
          "OM already initialized.Reusing existing cluster id for sd="
              + omStorage.getStorageDir() + ";cid=" + omStorage
              .getClusterID());
    }
    return new AlreadyInitialized();
  }



  private OMStorage storage;

  OMStorage getStorage(){
    return storage;
  }

  OzoneManagerStorageInitializer(OMStorage storage){
    this.storage = storage;
  }

  abstract void initialize(OzoneConfiguration conf) throws IOException;



  private static class AlreadyInitialized extends
      OzoneManagerStorageInitializer {
    AlreadyInitialized(){
      super(null);
    }

    @Override
    void initialize(OzoneConfiguration conf) {
      //do nothing
    }
  }
}
