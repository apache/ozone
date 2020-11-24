package org.apache.hadoop.ozone.s3.signature;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parser for getting auth info from query parameters.
 * <p>
 * See: https://docs.aws.amazon
 * .com/AmazonS3/latest/API/sigv4-query-string-auth.html
 */
public class AuthorizationV4QueryParser implements SignatureParser {

  private static final Logger LOG =
      LoggerFactory.getLogger(AuthorizationV4QueryParser.class);

  private final MultivaluedMap<String, String> queryParameters;

  public AuthorizationV4QueryParser(MultivaluedMap<String, String> queryParameters) {
    this.queryParameters = queryParameters;
  }

  @Override
  public SignatureInfo parseSignature() throws OS3Exception {

    if (!queryParameters.containsKey("X-Amz-Signature")) {
      return null;
    }

    Credential credential =
        new Credential(queryParameters.getFirst("X-Amz-Credential"));

    return new SignatureInfo(
        queryParameters.getFirst("X-Amz-Date"),
        credential.getAccessKeyID(),
        queryParameters.getFirst("X-Amz-Signature"),
        queryParameters.getFirst("X-Amz-SignedHeaders"),
        String.format("%s/%s/%s/%s", credential.getDate(),
            credential.getAwsRegion(), credential.getAwsService(),
            credential.getAwsRequest()),
        queryParameters.getFirst("X-Amz-Algorithm")
    );
  }
}
