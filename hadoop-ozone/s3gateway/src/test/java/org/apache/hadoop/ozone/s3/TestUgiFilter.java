/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import java.util.Map;
import java.util.HashMap;
import java.util.Enumeration;
import java.util.Collections;
import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.ozone.s3.signature.StringToSignProducer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test class for @{@link UgiFilter}.
 */
public class TestUgiFilter {

  private OzoneConfiguration conf;
  private String s3HttpAddr;
  private Text omService;
  private Map<String, String> headers;
  private Map<String, String[]> parameters;

  private final String hostHeader = "Host";
  private final String encodeHeader = "Accept-Encoding";
  private final String userAgentHeader = "User-Agent";
  private final String dateHeader = "X-Amz-Date";
  private final String encryptTypeHeader = "X-Amz-Content-SHA256";
  private final String authorizationHeader = "Authorization";
  private final String lengthHeader = "Content-Length";

  @Before
  public void setup() {
    conf = new OzoneConfiguration();
    s3HttpAddr = "localhost:9878";
    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY, s3HttpAddr);
    s3HttpAddr = s3HttpAddr.substring(0, s3HttpAddr.lastIndexOf(":"));
    conf.set(S3GatewayConfigKeys.OZONE_S3G_DOMAIN_NAME, s3HttpAddr);
    omService = new Text("127.0.0.1:9862");

    headers = new HashMap<>();
    parameters = new HashMap<>();

    headers.put(hostHeader, "localhost:9878");
    headers.put(encodeHeader, "identity");
    headers.put(userAgentHeader, "aws-cli/2.1.29 "+
        "Python/3.8.8 Linux/4.15.0-144-generic "+
        "exe/x86_64.ubuntu.18 prompt/off "+
        "command/s3api.create-bucket");
    headers.put(dateHeader, "20210616T195044Z");
    headers.put(encryptTypeHeader,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    headers.put(authorizationHeader, "AWS4-HMAC-SHA256 "+
        "Credential=AKIAIIIE56NH5ZHKLTWQ/20210616/us-east-1/s3/aws4_request, "+
        "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "+
        "Signature=c29b4c46e825d5df56cdde12a61adfa65560a54"+
        "7c8973b9809621086727a2f2e");
    headers.put(lengthHeader, "0");

  }

  @Test
  public void testUgiFilterDoFilter() throws IOException, ServletException {
    UgiFilter filter = new UgiFilter();
    filter.setOzoneConfiguration(this.conf);
    filter.setOmService(this.omService);

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    FilterChain filterChain = Mockito.mock(FilterChain.class);
    FilterConfig filterConfig = Mockito.mock(FilterConfig.class);

    Enumeration<String> headerNames = Collections.enumeration(headers.keySet());

    Mockito.when(request.getScheme()).thenReturn("http");
    Mockito.when(request.getMethod()).thenReturn("PUT");
    Mockito.when(request.getPathInfo()).thenReturn("/bucket1");
    Mockito.when(request.getHeaderNames()).thenReturn(headerNames);
    Mockito.when(request.getHeader(hostHeader)).thenReturn(
        headers.get(hostHeader));
    Mockito.when(request.getHeader(encodeHeader)).thenReturn(
        headers.get(encodeHeader));
    Mockito.when(request.getHeader(userAgentHeader)).thenReturn(
        headers.get(userAgentHeader));
    Mockito.when(request.getHeader(dateHeader)).thenReturn(
        headers.get(dateHeader));
    Mockito.when(request.getHeader(encryptTypeHeader)).thenReturn(
        headers.get(encryptTypeHeader));
    Mockito.when(request.getHeader(authorizationHeader)).thenReturn(
        headers.get(authorizationHeader));
    Mockito.when(request.getHeader(lengthHeader)).thenReturn(
        headers.get(lengthHeader));
    Mockito.when(request.getParameterMap()).thenReturn(parameters);

    filter.init(filterConfig);
    filter.doFilter(request, response, filterChain);
    filter.destroy();
  }

  @Test
  public void testUgiFilterStringToSign() throws OS3Exception, Exception {
    // test to ensure http servlet request is parsed correctly for
    // aws authenciation - testing creating aws v4 stringToSign
    AWSSignatureProcessor signature = new AWSSignatureProcessor(headers,
        parameters);

    SignatureInfo signatureInfo = signature.parseSignature();
    String stringToSign =
        StringToSignProducer.createSignatureBase(signatureInfo,
            "http",
            "PUT",
            "/bucket1",
            AWSSignatureProcessor.LowerCaseKeyStringMap.fromHeaderMap(headers),
            StringToSignProducer.fromMultiValueToSingleValueMap(
                parameters));

    Assert.assertEquals(
        "String to sign is invalid",
        "AWS4-HMAC-SHA256\n"+
            "20210616T195044Z\n"+
            "20210616/us-east-1/s3/aws4_request\n"+
            "7bed78b44380d69656995a5050761f6b88ee6ed9d8b6199e467b83ef931bae7b",
        stringToSign);
  }
}