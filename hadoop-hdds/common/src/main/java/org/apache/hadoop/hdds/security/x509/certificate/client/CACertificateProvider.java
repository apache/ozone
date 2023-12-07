package org.apache.hadoop.hdds.security.x509.certificate.client;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * An interface that defines a trust anchor provider API this class relies on.
 */
@FunctionalInterface
public interface CACertificateProvider {
  List<X509Certificate> provideCACerts() throws IOException;
}
