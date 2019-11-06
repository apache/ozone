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

public abstract class OzoneManagerInitializer {

  static final Logger LOG =
      LoggerFactory.getLogger(OzoneManager.class);

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

  private static OzoneManagerInitializer getImplementation(
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



  OMStorage storage;

  OzoneManagerInitializer(OMStorage storage){
    this.storage = storage;
  }

  abstract void initialize(OzoneConfiguration conf) throws IOException;



  private static class AlreadyInitialized extends OzoneManagerInitializer{
    AlreadyInitialized(){
      super(null);
    }

    @Override
    void initialize(OzoneConfiguration conf) {
      //do nothing
    }
  }
}
