/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3;

import javax.annotation.PostConstruct;
import javax.enterprise.context.RequestScoped;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.header.AuthorizationHeaderV2;
import org.apache.hadoop.ozone.s3.header.AuthorizationHeaderV4;
import org.apache.hadoop.ozone.s3.header.Credential;

import com.google.common.annotations.VisibleForTesting;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.S3_AUTHINFO_CREATION_ERROR;
import org.apache.kerby.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Parser to process AWS V2 & V4 auth request. Creates string to sign and auth
 * header. For more details refer to AWS documentation https://docs.aws
 * .amazon.com/general/latest/gr/sigv4-create-canonical-request.html.
 **/
@RequestScoped
public class AWSSignatureProcessor implements SignatureProcessor {

  private static final Logger LOG =
      LoggerFactory.getLogger(AWSSignatureProcessor.class);

  @Context
  private ContainerRequestContext context;

  private Map<String, String> headers;
  private MultivaluedMap<String, String> queryMap;
  private String uri;
  private String method;
  private AuthorizationHeaderV4 v4Header;
  private AuthorizationHeaderV2 v2Header;
  private String stringToSign;
  private Exception exception;

  @PostConstruct
  public void init()
      throws Exception {
    //header map is MUTABLE. It's better to save it here. (with lower case
    // keys!!!)
    this.headers = new LowerCaseKeyStringMap(new HashMap<>());
    for (Entry<String, List<String>> headerEntry : context.getHeaders()
        .entrySet()) {
      if (0 < headerEntry.getValue().size()) {
        String headerKey = headerEntry.getKey();
        if (headers.containsKey(headerKey)) {
          //mutiple headers from the same type are combined
          headers.put(headerKey,
              headers.get(headerKey) + "," + headerEntry.getValue().get(0));
        } else {
          headers.put(headerKey, headerEntry.getValue().get(0));
        }
      }
    }
    //in case of the HeaderPreprocessor executed before us, let's restore the
    // original content type.
    if (headers.containsKey(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE)) {
      headers.put(HeaderPreprocessor.CONTENT_TYPE,
          headers.get(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE));
    }


    this.queryMap = context.getUriInfo().getQueryParameters();
    this.uri = context.getUriInfo().getRequestUri().getPath();

    this.method = context.getMethod();
    String authHeader = headers.get(AUTHORIZATION_HEADER);
    try {
      if (authHeader != null) {
        String[] split = authHeader.split(" ");
        if (split[0].equals(AuthorizationHeaderV2.IDENTIFIER)) {
          if (v2Header == null) {
            v2Header = new AuthorizationHeaderV2(authHeader);
          }
        } else {
          if (v4Header == null) {
            v4Header = new AuthorizationHeaderV4(authHeader);
          }
          parse();
        }
      } else { // no auth header
        v4Header = null;
        v2Header = null;
      }
    } catch (Exception ex) {
      // During validation of auth header, create instance and set Exception.
      // This way it can be handled in OzoneClientProducer creation of
      // SignatureProcessor instance failure.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Error during Validation of Auth Header:{}", authHeader);
      }
      this.exception = ex;
    }
  }


  private void parse() throws Exception {

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
    String algorithm, requestDateTime, credentialScope, canonicalRequest;
    algorithm = v4Header.getAlgorithm();
    requestDateTime = headers.get(X_AMAZ_DATE);
    Credential credential = v4Header.getCredentialObj();
    credentialScope = String.format("%s/%s/%s/%s", credential.getDate(),
        credential.getAwsRegion(), credential.getAwsService(),
        credential.getAwsRequest());

    // If the absolute path is empty, use a forward slash (/)
    uri = (uri.trim().length() > 0) ? uri : "/";
    // Encode URI and preserve forward slashes
    strToSign.append(algorithm + NEWLINE);
    strToSign.append(requestDateTime + NEWLINE);
    strToSign.append(credentialScope + NEWLINE);

    canonicalRequest = buildCanonicalRequest();
    strToSign.append(hash(canonicalRequest));
    if (LOG.isDebugEnabled()) {
      LOG.debug("canonicalRequest:[{}]", canonicalRequest);
    }

    if (LOG.isTraceEnabled()) {
      headers.keySet().forEach(k -> LOG.trace("Header:{},value:{}", k,
          headers.get(k)));
    }

    LOG.debug("StringToSign:[{}]", strToSign);
    stringToSign = strToSign.toString();
  }

  @VisibleForTesting
  protected String buildCanonicalRequest() throws OS3Exception {

    Iterable<String> parts = split("/", uri);
    List<String> encParts = new ArrayList<>();
    for (String p : parts) {
      encParts.add(urlEncode(p));
    }
    String canonicalUri = join("/", encParts);

    String canonicalQueryStr = getQueryParamString();

    StringBuilder canonicalHeaders = new StringBuilder();

    for (String header : v4Header.getSignedHeaders()) {
      canonicalHeaders.append(header.toLowerCase());
      canonicalHeaders.append(":");
      if (headers.containsKey(header)) {
        String headerValue = headers.get(header);
        canonicalHeaders.append(headerValue);
        canonicalHeaders.append(NEWLINE);

        // Set for testing purpose only to skip date and host validation.
        validateSignedHeader(header, headerValue);

      } else {
        throw new RuntimeException("Header " + header + " not present in " +
            "request but requested to be signed.");
      }
    }

    String payloadHash;
    if (UNSIGNED_PAYLOAD.equals(
        headers.get(X_AMZ_CONTENT_SHA256))) {
      payloadHash = UNSIGNED_PAYLOAD;
    } else {
      payloadHash = headers.get(X_AMZ_CONTENT_SHA256);
    }

    String signedHeaderStr = v4Header.getSignedHeaderString();
    String canonicalRequest = method + NEWLINE
        + canonicalUri + NEWLINE
        + canonicalQueryStr + NEWLINE
        + canonicalHeaders + NEWLINE
        + signedHeaderStr + NEWLINE
        + payloadHash;

    return canonicalRequest;
  }

  @VisibleForTesting
  void validateSignedHeader(String header, String headerValue)
      throws OS3Exception {
    switch (header) {
    case HOST:
      try {
        String schema = context.getUriInfo().getRequestUri().getScheme();
        URI hostUri = new URI(schema + "://" + headerValue);
        InetAddress.getByName(hostUri.getHost());
        // TODO: Validate if current request is coming from same host.
      } catch (UnknownHostException|URISyntaxException e) {
        LOG.error("Host value mentioned in signed header is not valid. " +
            "Host:{}", headerValue);
        throw S3_AUTHINFO_CREATION_ERROR;
      }
      break;
    case X_AMAZ_DATE:
      LocalDate date = LocalDate.parse(headerValue, TIME_FORMATTER);
      LocalDate now = LocalDate.now();
      if (date.isBefore(now.minus(PRESIGN_URL_MAX_EXPIRATION_SECONDS, SECONDS))
          || date.isAfter(now.plus(PRESIGN_URL_MAX_EXPIRATION_SECONDS,
          SECONDS))) {
        LOG.error("AWS date not in valid range. Request timestamp:{} should " +
                "not be older than {} seconds.", headerValue,
            PRESIGN_URL_MAX_EXPIRATION_SECONDS);
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

  private String urlEncode(String str) {
    try {

      return URLEncoder.encode(str, StandardCharsets.UTF_8.name())
          .replaceAll("\\+", "%20")
          .replaceAll("%7E", "~");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private String getQueryParamString() {
    List<String> params = new ArrayList<>(queryMap.keySet());

    // Sort by name, then by value
    Collections.sort(params, (o1, o2) -> o1.equals(o2) ?
        queryMap.getFirst(o1).compareTo(queryMap.getFirst(o2)) :
        o1.compareTo(o2));

    StringBuilder result = new StringBuilder();
    for (String p : params) {
      if (result.length() > 0) {
        result.append("&");
      }
      result.append(urlEncode(p));
      result.append('=');

      result.append(urlEncode(queryMap.getFirst(p)));
    }
    return result.toString();
  }

  public static String hash(String payload) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(payload.getBytes(StandardCharsets.UTF_8));
    return Hex.encode(md.digest()).toLowerCase();
  }

  @Override
  public String getAwsAccessId() {
    return (v4Header != null ? v4Header.getAccessKeyID() :
        v2Header != null ? v2Header.getAccessKeyID() : "");
  }

  @Override
  public String getSignature() {
    return (v4Header != null ? v4Header.getSignature() :
        v2Header != null ? v2Header.getSignature() : "");
  }

  @Override
  public String getStringToSign() throws Exception {
    return stringToSign;
  }

  @VisibleForTesting
  public void setContext(ContainerRequestContext context) {
    this.context = context;
  }

  @VisibleForTesting
  public void setV4Header(
      AuthorizationHeaderV4 v4Header) {
    this.v4Header = v4Header;
  }

  @VisibleForTesting
  public void setV2Header(AuthorizationHeaderV2 v2Header) {
    this.v2Header = v2Header;
  }

  @Override
  public Exception getException() {
    return this.exception;
  }

  /**
   * A simple map which forces lower case key usage.
   */
  public static class LowerCaseKeyStringMap implements Map<String, String> {

    private HashMap<String, String> delegate;

    public LowerCaseKeyStringMap(
        HashMap<String, String> delegate) {
      this.delegate = delegate;
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public boolean isEmpty() {
      return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
      return delegate.containsKey(key.toString().toLowerCase());
    }

    @Override
    public boolean containsValue(Object value) {
      return delegate.containsValue(value);
    }

    @Override
    public String get(Object key) {
      return delegate.get(key.toString().toLowerCase());
    }

    @Override
    public String put(String key, String value) {
      return delegate.put(key.toLowerCase(), value);
    }

    @Override
    public String remove(Object key) {
      return delegate.remove(key.toString());
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
      for (Entry<? extends String, ? extends String> entry : m.entrySet()) {
        put(entry.getKey().toLowerCase(), entry.getValue());
      }
    }

    @Override
    public void clear() {
      delegate.clear();
    }

    @Override
    public Set<String> keySet() {
      return delegate.keySet();
    }

    @Override
    public Collection<String> values() {
      return delegate.values();
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
      return delegate.entrySet();
    }
  }

}
