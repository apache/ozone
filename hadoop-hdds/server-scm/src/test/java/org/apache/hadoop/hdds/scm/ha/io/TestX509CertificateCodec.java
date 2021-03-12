package org.apache.hadoop.hdds.scm.ha.io;

import com.google.protobuf.ByteString;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.security.KeyPair;
import java.security.cert.X509Certificate;

public class TestX509CertificateCodec {

  @Test
  public void codec() throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate x509Certificate =
        KeyStoreTestUtil.generateCertificate("CN=Test", keyPair, 30,
        "SHA256withRSA");

    X509CertificateCodec x509CertificateCodec = new X509CertificateCodec();
    ByteString byteString = x509CertificateCodec.serialize(x509Certificate);

    X509Certificate actual = (X509Certificate)
        x509CertificateCodec.deserialize(X509Certificate.class, byteString);

    Assert.assertEquals(x509Certificate, actual);


  }
}
