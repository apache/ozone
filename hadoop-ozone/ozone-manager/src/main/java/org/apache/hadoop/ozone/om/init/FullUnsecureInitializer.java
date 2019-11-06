package org.apache.hadoop.ozone.om.init;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.ScmClient;

import java.io.IOException;

class FullUnsecureInitializer extends OzoneManagerInitializer {

  FullUnsecureInitializer(OMStorage storage) {
    super(storage);
  }

  @Override
  void initialize(OzoneConfiguration conf) throws IOException {
    final ScmClient client = new ScmClient(conf);
    ScmInfo scmInfo = client.getScmInfo();
    String clusterId = scmInfo.getClusterId();
    String scmId = scmInfo.getScmId();
    if (clusterId == null || clusterId.isEmpty()) {
      throw new IOException("Invalid Cluster ID");
    }
    if (scmId == null || scmId.isEmpty()) {
      throw new IOException("Invalid SCM ID");
    }
    storage.setClusterId(clusterId);
    storage.setScmId(scmId);
    storage.initialize();
    System.out.println(
        "OM initialization succeeded. Current cluster id for Storage Directory="
            + storage.getStorageDir() + "; Cluster ID=" + storage
            .getClusterID());
  }

}
