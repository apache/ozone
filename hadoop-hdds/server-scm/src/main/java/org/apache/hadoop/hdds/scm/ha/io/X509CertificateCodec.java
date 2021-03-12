package org.apache.hadoop.hdds.scm.ha.io;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;

import java.security.cert.X509Certificate;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Codec for type X509Certificate.
 */
public class X509CertificateCodec implements Codec {
  @Override
  public ByteString serialize(Object object)
      throws InvalidProtocolBufferException {
    try {
      String certString =
          CertificateCodec.getPEMEncodedString((X509Certificate) object);
      return ByteString.copyFrom(certString.getBytes(UTF_8));
    } catch (Exception ex) {
      throw new InvalidProtocolBufferException(
          "X509Certificate cannot be decoded: " + ex.getMessage());
    }
  }

  @Override
  public Object deserialize(Class< ? > type, ByteString value)
      throws InvalidProtocolBufferException {
    try {
      String pemEncodedCert = new String(value.toByteArray(), UTF_8);
      return CertificateCodec.getX509Certificate(pemEncodedCert);
    } catch (Exception ex) {
      throw new InvalidProtocolBufferException(
          "X509Certificate cannot be decoded: " + ex.getMessage());
    }
  }
}

