package org.apache.hadoop.fs.ozone;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Hadoop RPC based transport (wihout HA support).
 */
public class Hadoop27RpcTransport implements OmTransport {

  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final OzoneManagerProtocolPB proxy;

  private ConfigurationSource conf;

  public Hadoop27RpcTransport(
      ConfigurationSource conf) throws IOException {
    this.conf = conf;
    InetSocketAddress socket = OmUtils.getOmAddressForClients(conf);
    long version = RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    OzoneConfiguration ozoneConfiguration = OzoneConfiguration.of(conf);
    RPC.setProtocolEngine(ozoneConfiguration,
        OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    proxy = RPC.getProtocolProxy(OzoneManagerProtocolPB.class, version,
        socket, UserGroupInformation.getCurrentUser(),
        ozoneConfiguration,
        NetUtils.getDefaultSocketFactory(ozoneConfiguration),
        RPC.getRpcTimeout(ozoneConfiguration), null).getProxy();
  }

  @Override
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    try {
      return proxy.submitRequest(NULL_RPC_CONTROLLER, payload);
    } catch (ServiceException e) {
      throw new IOException("Service exception during the OM call", e);
    }
  }

  @Override
  public Text getDelegationTokenService() {
    return null;
  }

}
