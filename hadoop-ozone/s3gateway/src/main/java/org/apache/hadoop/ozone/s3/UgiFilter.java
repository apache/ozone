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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.ozone.s3.signature.StringToSignProducer;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Enumeration;
import org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor;

import com.google.common.annotations.VisibleForTesting;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_HEADER;

/**
 * Preprocessing filter for every request.
 * - creates OzoneToken containing aws signature
 * aws id and stringToSign for aws authenication;  stores OzoneToken in
 * thread local variable (UserGroupInformation object) avail to all
 * s3 rest command endpoints
 */
public class UgiFilter implements Filter {
  public static final Logger LOG = LoggerFactory.getLogger(UgiFilter.class);

  @Inject
  private OzoneConfiguration ozoneConfiguration;
  @Inject
  private Text omService;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {

  }

  @Override
  public void doFilter(ServletRequest servletRequest,
                       ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException {
    Map<String, String> headerMap = new HashMap<>();
    HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
    Enumeration<String> headerNames = httpRequest.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String headerKey = headerNames.nextElement();
      headerMap.put(headerKey, httpRequest.getHeader(headerKey));
      LOG.info("request {} : {}", headerKey, httpRequest.getHeader(headerKey));
    }
    AWSSignatureProcessor signature = new AWSSignatureProcessor(headerMap,
        httpRequest.getParameterMap());
    SignatureInfo signatureInfo;
    String stringToSign = "";
    String awsAccessId = "";
    try {
      signatureInfo = signature.parseSignature();
      if (signatureInfo.getVersion() == SignatureInfo.Version.V4) {
        stringToSign =
            StringToSignProducer.createSignatureBase(signatureInfo,
                httpRequest.getScheme(),
                httpRequest.getMethod(),
                httpRequest.getPathInfo(),
                AWSSignatureProcessor.LowerCaseKeyStringMap
                    .fromHeaderMap(headerMap),
                StringToSignProducer.fromMultiValueToSingleValueMap(
                    httpRequest.getParameterMap()));
      }
      awsAccessId = signatureInfo.getAwsAccessId();
      validateAccessId(awsAccessId);
    } catch (Throwable t) {
      throw new IOException(t);
    }

    UserGroupInformation remoteUser =
        UserGroupInformation.createRemoteUser(awsAccessId);

    if (OzoneSecurityUtil.isSecurityEnabled(ozoneConfiguration)) {
      LOG.debug("Creating s3 auth info for client.");

      if (signatureInfo.getVersion() == SignatureInfo.Version.NONE) {
        //throw MALFORMED_HEADER;
        throw new IOException("MALFORMED_HEADER");
      }

      OzoneTokenIdentifier identifier = new OzoneTokenIdentifier();
      identifier.setTokenType(S3AUTHINFO);
      identifier.setStrToSign(stringToSign);
      identifier.setSignature(signatureInfo.getSignature());
      identifier.setAwsAccessId(awsAccessId);
      identifier.setOwner(new Text(awsAccessId));
      if (LOG.isTraceEnabled()) {
        LOG.trace("Adding token for service:{}", omService);
      }
      Token<OzoneTokenIdentifier> token = new Token(identifier.getBytes(),
          identifier.getSignature().getBytes(StandardCharsets.UTF_8),
          identifier.getKind(),
          omService);
      remoteUser.addToken(token);
    }
    try {
      remoteUser.doAs((PrivilegedExceptionAction<Void>) () -> {
        filterChain.doFilter(httpRequest, servletResponse);
        return null;
      });
    } catch (InterruptedException e) {
      throw new IOException("Interrupted thread call doAs", e);
    }
  }

  @Override
  public void destroy() { }

  private WebApplicationException wrapOS3Exception(OS3Exception os3Exception) {
    return new WebApplicationException(os3Exception,
        os3Exception.getHttpCode());
  }

  private void validateAccessId(String awsAccessId) throws Exception {
    if (awsAccessId == null || awsAccessId.equals("")) {
      LOG.error("Malformed s3 header. awsAccessID: ", awsAccessId);
      throw wrapOS3Exception(MALFORMED_HEADER);
    }
  }
  @VisibleForTesting
  public void setOzoneConfiguration(OzoneConfiguration config) {
    this.ozoneConfiguration = config;
  }

  @VisibleForTesting
  public void setOmService(Text omService) {
    this.omService = omService;
  }

}
