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

import org.apache.http.NameValuePair;

import java.util.List;

/**
 * This is wrapper class for http request.
 */
public class HttpRequestWrapper {
  private String host;
  private int port;
  private String uri;
  private List<NameValuePair> urlParameters;
  private HttpReqType httpReqType;

  public HttpRequestWrapper(String host, int port,
                            String uri, List<NameValuePair> urlParameters,
                            HttpReqType httpReqType) {
    this.host = host;
    this.port = port;
    this.uri = uri;
    this.urlParameters = urlParameters;
    this.httpReqType = httpReqType;
  }

  /**
   * Enum for Http Request Type - GET and POST.
   */
  public enum HttpReqType {
    POST("post"),
    GET("get");

    private final String label;

    HttpReqType(String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public List<NameValuePair> getUrlParameters() {
    return urlParameters;
  }

  public HttpReqType getHttpReqType() {
    return httpReqType;
  }

  public String getUri() {
    return uri;
  }

}
