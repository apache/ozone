package org.apache.hadoop.ozone.om.bucket.server;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.bucket.server.ratis.XbeiverServerRatis;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Ozone main class sets up the network servers and initializes the bucket
 * layer.
 */
public class OzoneBucket {

  private static final Logger LOG = LoggerFactory.getLogger(
      OzoneBucket.class);
  private final XbeiverServerSpi writeChannel;
  private final GrpcTlsConfig tlsClientConfig;
  private final OzoneManager ozoneManager;

  public OzoneBucket(OzoneManager ozoneManager, ConfigurationSource conf) {
    this.ozoneManager = ozoneManager;
    try {
      this.writeChannel = XbeiverServerRatis.newXbeiverServerRatis(ozoneManager, conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    CertificateClient certClient = ozoneManager.getCertificateClient();
    SecurityConfig secConf = new SecurityConfig(conf);

    if (certClient != null && secConf.isGrpcTlsEnabled()) {
      try {
        tlsClientConfig = new GrpcTlsConfig(
            certClient.getKeyManager(),
            certClient.getTrustManager(), true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      tlsClientConfig = null;
    }
  }

  /**
   * Starts serving requests to ozone bucket.
   *
   * @throws IOException
   */
  public void start(String clusterId) throws IOException {
    writeChannel.start();
  }

  /**
   * Stop Bucket Service on the datanode.
   */
  public void stop() {
    writeChannel.stop();
  }

  public XbeiverServerSpi getWriteChannel() {
    return writeChannel;
  }

  public GrpcTlsConfig getTlsClientConfig() {
    return tlsClientConfig;
  }
}
