package org.apache.hadoop.ozone.om.init;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMStorage;

import java.io.IOException;

class FullSecureInitializer extends SecurityInitializer {

  FullSecureInitializer(OMStorage storage) {
    super(storage);
  }

  @Override
  void initialize(OzoneConfiguration conf) throws IOException {
    new FullUnsecureInitializer(storage).initialize(conf);
    new SecurityInitializer(storage).initialize(conf);
  }
}
