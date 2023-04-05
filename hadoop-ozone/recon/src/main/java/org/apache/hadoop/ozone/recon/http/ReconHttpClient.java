/*
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
package org.apache.hadoop.ozone.recon.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * This class is used to make http
 * GET and POST requests to Apache Solr.
 */
@Singleton
public class ReconHttpClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReconHttpClient.class);

  public static CloseableHttpClient getCloseableHttpClient() {
    CloseableHttpClient httpClient = null;
    try {
      System.setProperty("sun.security.krb5.debug", "true");
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
      Lookup<AuthSchemeProvider> authSchemeRegistry =
          RegistryBuilder.<AuthSchemeProvider>create()
              .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true))
              .build();
      httpClient = HttpClients.custom()
          .setSSLHostnameVerifier(
              NoopHostnameVerifier.INSTANCE)
          .setSSLContext(new SSLContextBuilder().loadTrustMaterial(
              null, new TrustStrategy() {
                public boolean isTrusted(X509Certificate[] arg0, String arg1)
                    throws CertificateException {
                  return true;
                }
              }).build())
          .setDefaultAuthSchemeRegistry(authSchemeRegistry).build();
    } catch (KeyManagementException e) {
      LOG.error("KeyManagementException in creating http client instance", e);
    } catch (NoSuchAlgorithmException e) {
      LOG.error("NoSuchAlgorithmException in creating http client instance", e);
    } catch (KeyStoreException e) {
      LOG.error("KeyStoreException in creating http client instance", e);
    }
    return httpClient;
  }

  private static HttpClientContext getHttpClientContext() {
    HttpClientContext context = HttpClientContext.create();
    BasicCredentialsProvider credentialsProvider =
        new BasicCredentialsProvider();
    // This may seem odd, but specifying 'null' as principal
    // tells java to use the logged in user's credentials
    Credentials useJaasCreds = new Credentials() {
      public String getPassword() {
        return null;
      }

      public Principal getUserPrincipal() {
        return null;
      }
    };
    credentialsProvider.setCredentials(
        new AuthScope(null, -1, null), useJaasCreds);
    context.setCredentialsProvider(credentialsProvider);
    return context;
  }

  public String sendPost(String host, int port,
                         String uri, List<NameValuePair> urlParameters)
      throws IOException {
    String entityResponse = "";
    String solrHostBaseQuery = getSolrHostBaseURI(host, port, uri);
    HttpPost post = new HttpPost(solrHostBaseQuery);
    LOG.info("Making Http Query to Solr: {} ", solrHostBaseQuery);
    post.setEntity(new UrlEncodedFormEntity(urlParameters));
    try (CloseableHttpClient httpClient = getCloseableHttpClient();
         CloseableHttpResponse response = httpClient.execute(post,
             getHttpClientContext())) {
      int statusCode = response.getStatusLine().getStatusCode();
      LOG.info("Status Code: {}", statusCode);
      //Verify response
      if (statusCode == 200) {
        entityResponse = EntityUtils.toString(response.getEntity());
      }
    }
    return entityResponse;
  }

  private String getSolrHostBaseURI(String host, int port, String uri) {
    StringBuilder urlBuilder = new StringBuilder("https://");
    urlBuilder.append(host);
    urlBuilder.append(":");
    urlBuilder.append(port);
    urlBuilder.append(uri);
    return urlBuilder.toString();
  }

  public String sendRequest(HttpRequestWrapper httpRequestWrapper)
      throws IOException {
    String entityResponse = StringUtils.EMPTY;
    HttpRequestWrapper.HttpReqType httpReqType =
        httpRequestWrapper.getHttpReqType();
    switch (httpReqType) {
    case POST:
      entityResponse = sendPost(httpRequestWrapper.getHost(),
          httpRequestWrapper.getPort(), httpRequestWrapper.getUri(),
          httpRequestWrapper.getUrlParameters());
      break;
    case GET:
      entityResponse = sendGet(httpRequestWrapper.getHost(),
          httpRequestWrapper.getPort(), httpRequestWrapper.getUri(),
          httpRequestWrapper.getUrlParameters());
      break;
    default:
      LOG.error("Unsupported Http Request Type: {} ", httpReqType);
      break;
    }
    return entityResponse;
  }

  private String sendGet(String host, int port, String uri,
                         List<NameValuePair> urlParameters) {
    return StringUtils.EMPTY;
  }
}
