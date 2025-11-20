/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.signature;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.S3_AUTHINFO_CREATION_ERROR;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;

import com.google.common.annotations.VisibleForTesting;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor.LowerCaseKeyStringMap;
import org.apache.hadoop.ozone.s3.util.S3Utils;
import org.apache.kerby.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless utility to create stringToSign, the base of the signature.
 */
public final class StringToSignProducer {

  public static final String X_AMAZ_DATE = "x-amz-date";
  private static final Logger LOG =
      LoggerFactory.getLogger(StringToSignProducer.class);
  private static final Charset UTF_8 = StandardCharsets.UTF_8;
  private static final String NEWLINE = "\n";
  public static final String HOST = "host";
  /**
   * Seconds in a week, which is the max expiration time Sig-v4 accepts.
   */
  private static final long PRESIGN_URL_MAX_EXPIRATION_SECONDS =
      60 * 60 * 24 * 7;
  public static final DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
          .withZone(ZoneOffset.UTC);

  private StringToSignProducer() {
  }

  public static String createSignatureBase(
      SignatureInfo signatureInfo,
      ContainerRequestContext context
  ) throws Exception {
    return createSignatureBase(signatureInfo,
        context.getUriInfo().getRequestUri().getScheme(),
        context.getMethod(),
        LowerCaseKeyStringMap.fromHeaderMap(context.getHeaders()),
        fromMultiValueToSingleValueMap(
            context.getUriInfo().getQueryParameters()));
  }

  @VisibleForTesting
  public static String createSignatureBase(
      SignatureInfo signatureInfo,
      String scheme,
      String method,
      LowerCaseKeyStringMap headers,
      Map<String, String> queryParams
  ) throws Exception {
    StringBuilder strToSign = new StringBuilder();
    // According to AWS sigv4 documentation, authorization header should be
    // in following format.
    // Authorization: algorithm Credential=access key ID/credential scope,
    // SignedHeaders=SignedHeaders, Signature=signature

    // Construct String to sign in below format.
    // StringToSign =
    //    Algorithm + \n +
    //    RequestDateTime + \n +
    //    CredentialScope + \n +
    //    HashedCanonicalRequest
    String credentialScope = signatureInfo.getCredentialScope();

    // If the absolute path is empty, use a forward slash (/)
    String uri = signatureInfo.getUnfilteredURI();
    uri = StringUtils.isNotBlank(uri) ? uri : "/";
    // Encode URI and preserve forward slashes
    strToSign.append(signatureInfo.getAlgorithm()).append(NEWLINE);
    if (signatureInfo.getDateTime() == null) {
      LOG.error("DateTime Header not found.");
      throw S3_AUTHINFO_CREATION_ERROR;
    }
    strToSign.append(signatureInfo.getDateTime()).append(NEWLINE);
    strToSign.append(credentialScope).append(NEWLINE);
    String canonicalRequest = buildCanonicalRequest(
        scheme,
        method,
        uri,
        signatureInfo.getSignedHeaders(),
        headers,
        queryParams,
        signatureInfo.getPayloadHash());
    strToSign.append(hash(canonicalRequest));
    if (LOG.isDebugEnabled()) {
      LOG.debug("canonicalRequest:[{}]", canonicalRequest);
      LOG.debug("StringToSign:[{}]", strToSign);
    }

    return strToSign.toString();
  }

  public static Map<String, String> fromMultiValueToSingleValueMap(
      MultivaluedMap<String, String> queryParameters
  ) {
    Map<String, String> result = new HashMap<>();
    for (String key : queryParameters.keySet()) {
      result.put(key, queryParameters.getFirst(key));
    }
    return result;
  }

  public static String hash(String payload) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(payload.getBytes(UTF_8));
    return Hex.encode(md.digest()).toLowerCase();
  }

  @VisibleForTesting
  public static String buildCanonicalRequest(
      String schema,
      String method,
      String uri,
      String signedHeaders,
      Map<String, String> headers,
      Map<String, String> queryParams,
      String payloadHash
  ) throws OS3Exception {

    Iterable<String> parts = split("/", uri);
    List<String> encParts = new ArrayList<>();
    for (String p : parts) {
      encParts.add(urlEncode(p));
    }
    String canonicalUri = join("/", encParts);

    String canonicalQueryStr = getQueryParamString(queryParams);

    StringBuilder canonicalHeaders = new StringBuilder();

    for (String header : StringUtils.split(signedHeaders, ';')) {
      canonicalHeaders.append(header.toLowerCase());
      canonicalHeaders.append(':');
      if (headers.containsKey(header)) {
        String headerValue = headers.get(header);
        if (header.equals("content-type")) {
          headerValue = headerValue.toLowerCase();
        }
        canonicalHeaders.append(headerValue);
        canonicalHeaders.append(NEWLINE);

        // Set for testing purpose only to skip date and host validation.
        try {
          validateSignedHeader(schema, header, headerValue);
        } catch (DateTimeParseException ex) {
          LOG.error("DateTime format invalid.", ex);
          throw S3_AUTHINFO_CREATION_ERROR;
        }

      } else {
        LOG.error("Header " + header + " not present in "
            + "request but requested to be signed.");
        throw S3_AUTHINFO_CREATION_ERROR;
      }
    }

    validateCanonicalHeaders(canonicalHeaders.toString(), headers);

    return method + NEWLINE
        + canonicalUri + NEWLINE
        + canonicalQueryStr + NEWLINE
        + canonicalHeaders + NEWLINE
        + signedHeaders + NEWLINE
        + payloadHash;
  }

  /**
   * String join that also works with empty strings.
   *
   * @return joined string
   */
  private static String join(String glue, List<String> parts) {
    StringBuilder result = new StringBuilder();
    boolean addSeparator = false;
    for (String p : parts) {
      if (addSeparator) {
        result.append(glue);
      }
      result.append(p);
      addSeparator = true;
    }
    return result.toString();
  }

  /**
   * Returns matching strings.
   *
   * @param regex Regular expression to split by
   * @param whole The string to split
   * @return pieces
   */
  private static Iterable<String> split(String regex, String whole) {
    Pattern p = Pattern.compile(regex);
    Matcher m = p.matcher(whole);
    List<String> result = new ArrayList<>();
    int pos = 0;
    while (m.find()) {
      result.add(whole.substring(pos, m.start()));
      pos = m.end();
    }
    result.add(whole.substring(pos));
    return result;
  }

  private static String urlEncode(String str) {
    try {
      return S3Utils.urlEncode(str)
          .replaceAll("\\+", "%20")
          .replaceAll("\\*", "%2A")
          .replaceAll("%7E", "~");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getQueryParamString(
      Map<String, String> queryMap
  ) {
    List<String> params = new ArrayList<>(queryMap.keySet());

    // Sort by name, then by value
    Collections.sort(params, (o1, o2) -> o1.equals(o2) ?
        queryMap.get(o1).compareTo(queryMap.get(o2)) :
        o1.compareTo(o2));

    StringBuilder result = new StringBuilder();
    for (String p : params) {
      if (!p.equals("X-Amz-Signature")) {
        if (result.length() > 0) {
          result.append('&');
        }
        result.append(urlEncode(p));
        result.append('=');

        result.append(urlEncode(queryMap.get(p)));
      }
    }
    return result.toString();
  }

  @VisibleForTesting
  static void validateSignedHeader(
      String schema,
      String header,
      String headerValue
  ) throws OS3Exception, DateTimeParseException {
    switch (header) {
    case HOST:
      break;
    case X_AMAZ_DATE:
      LocalDateTime date = LocalDateTime.parse(headerValue, TIME_FORMATTER);
      LocalDateTime now = LocalDateTime.now();
      if (date.isBefore(now.minus(PRESIGN_URL_MAX_EXPIRATION_SECONDS, SECONDS))
          || date.isAfter(now.plus(PRESIGN_URL_MAX_EXPIRATION_SECONDS,
          SECONDS))) {
        LOG.error("AWS date not in valid range. Request timestamp:{} should "
            + "not be older than {} seconds.",
            headerValue, PRESIGN_URL_MAX_EXPIRATION_SECONDS);
        throw S3_AUTHINFO_CREATION_ERROR;
      }
      break;
    case X_AMZ_CONTENT_SHA256:
      // TODO: Construct request payload and match HEX(SHA256(requestPayload))
      break;
    default:
      break;
    }
  }

  /**
   * According to AWS Sig V4 documentation:
   * https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
   * The CanonicalHeaders list must include the following:
   * HTTP host header..
   * Any x-amz-* headers that you plan to include
   * in your request must also be added.
   *
   * @param canonicalHeaders
   * @param headers
   */
  private static void validateCanonicalHeaders(
      String canonicalHeaders,
      Map<String, String> headers)
      throws OS3Exception {
    if (!canonicalHeaders.contains(HOST + ":")) {
      LOG.error("The SignedHeaders list must include HTTP Host header");
      throw S3_AUTHINFO_CREATION_ERROR;
    }
    for (String header : headers.keySet().stream()
        .filter(s -> s.startsWith("x-amz-"))
        .collect(Collectors.toSet())) {
      if (!(canonicalHeaders.contains(header + ":"))) {
        // According to AWS Signature V4 documentation using Authorization Header
        // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
        // The x-amz-content-sha256 header is not required for CanonicalHeaders
        if (X_AMZ_CONTENT_SHA256.equals(header)) {
          continue;
        }
        LOG.error("The SignedHeaders list must include all "
            + "x-amz-* headers in the request");
        throw S3_AUTHINFO_CREATION_ERROR;
      }
    }
  }

}
