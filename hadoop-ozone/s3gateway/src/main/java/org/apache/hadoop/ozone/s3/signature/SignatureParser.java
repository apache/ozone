package org.apache.hadoop.ozone.s3.signature;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;

public interface SignatureParser {

  String AUTHORIZATION_HEADER = "Authorization";

  SignatureInfo parseSignature() throws OS3Exception;
}
