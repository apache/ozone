package org.apache.hadoop.fs.ozone;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * OM Transport factory to create Simple (NON-HA) OM transport client.
 */
public class Hadoop27OmTransportFactory implements OmTransportFactory {

  @Override
  public OmTransport createOmTransport(ConfigurationSource source,
      UserGroupInformation ugi, String omServiceId) throws IOException {
    return new Hadoop27RpcTransport(source);
  }

}
