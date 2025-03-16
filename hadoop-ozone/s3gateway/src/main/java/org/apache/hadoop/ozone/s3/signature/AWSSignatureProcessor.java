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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_HEADER;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.enterprise.context.RequestScoped;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.s3.HeaderPreprocessor;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo.Version;
import org.apache.hadoop.ozone.s3.util.AuditUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parser to process AWS V2 and V4 auth request. Creates string to sign and auth
 * header. For more details refer to AWS documentation https://docs.aws
 * .amazon.com/general/latest/gr/sigv4-create-canonical-request.html.
 **/
@RequestScoped
public class AWSSignatureProcessor implements SignatureProcessor {

  private static final Logger LOG =
      LoggerFactory.getLogger(AWSSignatureProcessor.class);

  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.S3GLOGGER);

  @Context
  private ContainerRequestContext context;

  @Override
  public SignatureInfo parseSignature() throws OS3Exception {

    LowerCaseKeyStringMap headers =
        LowerCaseKeyStringMap.fromHeaderMap(context.getHeaders());

    String authHeader = headers.get("Authorization");

    List<SignatureParser> signatureParsers = new ArrayList<>();
    signatureParsers.add(new AuthorizationV4HeaderParser(authHeader,
        headers.get(StringToSignProducer.X_AMAZ_DATE)));
    signatureParsers.add(new AuthorizationV4QueryParser(
        StringToSignProducer.fromMultiValueToSingleValueMap(
            context.getUriInfo().getQueryParameters())));
    signatureParsers.add(new AuthorizationV2HeaderParser(authHeader));

    SignatureInfo signatureInfo = null;
    for (SignatureParser parser : signatureParsers) {
      try {
        signatureInfo = parser.parseSignature();
      } catch (MalformedResourceException e) {
        AuditMessage message = buildAuthFailureMessage(e);
        AUDIT.logAuthFailure(message);
        throw S3ErrorTable.newError(MALFORMED_HEADER, e.getResource());
      }
      if (signatureInfo != null) {
        break;
      }
    }
    if (signatureInfo == null) {
      signatureInfo = new SignatureInfo.Builder(Version.NONE).build();
    }
    signatureInfo.setUnfilteredURI(
        context.getUriInfo().getRequestUri().getPath());
    return signatureInfo;
  }

  private AuditMessage buildAuthFailureMessage(MalformedResourceException e) {
    AuditMessage message = new AuditMessage.Builder()
        .forOperation(AuthOperation.fromContext(context))
        .withParams(AuditUtils.getAuditParameters(context))
        .atIp(AuditUtils.getClientIpAddress(context))
        .withResult(AuditEventStatus.FAILURE)
        .withException(e)
        .build();
    return message;
  }

  @VisibleForTesting
  public void setContext(ContainerRequestContext context) {
    this.context = context;
  }

  /**
   * A simple map which forces lower case key usage.
   */
  public static class LowerCaseKeyStringMap implements Map<String, String> {

    private Map<String, String> delegate;

    public LowerCaseKeyStringMap() {
      this.delegate = new HashMap<>();
    }

    public static LowerCaseKeyStringMap fromHeaderMap(
        MultivaluedMap<String,
            String> rawHeaders
    ) {

      //header map is MUTABLE. It's better to save it here. (with lower case
      // keys!!!)
      final LowerCaseKeyStringMap headers =
          new LowerCaseKeyStringMap();

      for (Entry<String, List<String>> headerEntry : rawHeaders.entrySet()) {
        if (!headerEntry.getValue().isEmpty()) {
          String headerKey = headerEntry.getKey();
          if (headers.containsKey(headerKey)) {
            //multiple headers from the same type are combined
            headers.put(headerKey,
                headers.get(headerKey) + "," + headerEntry.getValue().get(0));
          } else {
            headers.put(headerKey, headerEntry.getValue().get(0));
          }
        }
      }

      headers.fixContentType();

      if (LOG.isTraceEnabled()) {
        headers.keySet().forEach(k -> LOG.trace("Header:{},value:{}", k,
            headers.get(k)));
      }
      return headers;
    }

    @VisibleForTesting
    protected void fixContentType() {
      //in case of the HeaderPreprocessor executed before us, let's restore the
      // original content type.
      if (containsKey(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE)) {
        put(HeaderPreprocessor.CONTENT_TYPE,
            get(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE));
      }
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
