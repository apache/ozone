package org.apache.hadoop.ozone.om.protocolPB;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.security.UserGroupInformation;

public interface OmTransportFactory {
  OmTransport getOmTransport();

  static OmTransport create(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId) throws IOException {
    return new Hadoop32RpcOmTransport(conf, ugi, omServiceId);
  }
}
