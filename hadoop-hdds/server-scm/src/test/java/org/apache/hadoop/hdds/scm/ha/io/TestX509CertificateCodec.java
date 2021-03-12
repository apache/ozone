package org.apache.hadoop.hdds.scm.ha.io;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.security.KeyPair;
import java.security.cert.X509Certificate;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Class to test X509CertificateCodec serialize and deserialize.
 */
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

  @Test(expected = InvalidProtocolBufferException.class)
  public void testCodecError() throws Exception {

    X509CertificateCodec x509CertificateCodec = new X509CertificateCodec();
    ByteString byteString = ByteString.copyFrom("dummy".getBytes(UTF_8));

    X509Certificate actual = (X509Certificate)
        x509CertificateCodec.deserialize(X509Certificate.class, byteString);
  }
}
