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
package org.apache.hadoop.ozone.s3.signature;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.ozone.s3.HeaderPreprocessor;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo.Version;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parser to process AWS V2 & V4 auth request. Creates string to sign and auth
 * header. For more details refer to AWS documentation https://docs.aws
 * .amazon.com/general/latest/gr/sigv4-create-canonical-request.html.
 **/

public class AWSSignatureProcessor implements SignatureProcessor {

  private static final Logger LOG =
      LoggerFactory.getLogger(AWSSignatureProcessor.class);

  private Map<String, String> headerMap;
  private Map<String, String[]> parameterMap;

  public AWSSignatureProcessor(Map<String, String> headerMap,
                               Map<String, String[]> parameterMap) {
    this.headerMap = headerMap;
    this.parameterMap = parameterMap;
  }
  public SignatureInfo parseSignature() throws OS3Exception {

    LowerCaseKeyStringMap headers =
        LowerCaseKeyStringMap.fromHeaderMap(headerMap);

    String authHeader = headers.get("Authorization");

    List<SignatureParser> signatureParsers = new ArrayList<>();
    signatureParsers.add(new AuthorizationV4HeaderParser(authHeader,
        headers.get(StringToSignProducer.X_AMAZ_DATE)));
    signatureParsers.add(new AuthorizationV4QueryParser(
        StringToSignProducer.fromMultiValueToSingleValueMap(
            parameterMap)));
    signatureParsers.add(new AuthorizationV2HeaderParser(authHeader));

    SignatureInfo signatureInfo = null;
    for (SignatureParser parser : signatureParsers) {
      signatureInfo = parser.parseSignature();
      if (signatureInfo != null) {
        break;
      }
    }
    if (signatureInfo == null) {
      signatureInfo = new SignatureInfo(
          Version.NONE,
          "", "", "", "", "", "", "", false
      );
    }
    return signatureInfo;
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
        Map<String,
            String> rawHeaders
    ) {
      //header map is MUTABLE. It's better to save it here. (with lower case
      // keys!!!)
      final LowerCaseKeyStringMap headers =
          new LowerCaseKeyStringMap();
      rawHeaders.forEach((k, v) -> {
        if (headers.containsKey(k)) {
          headers.put(k, headers.get(k) + "," + v);
        } else {
          headers.put(k, v);
        }
      });
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
