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

package org.apache.hadoop.hdds.server.http.servletbridge;

import java.io.BufferedReader;
import java.io.IOException;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;
import javax.servlet.AsyncContext;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpUpgradeHandler;
import javax.servlet.http.Part;

/**
 * Exposes a {@link jakarta.servlet.http.HttpServletRequest} as a
 * {@code javax.servlet.http.HttpServletRequest} so that hadoop's
 * {@code javax.servlet}-based authentication filters can read the request
 * inside a Jetty EE10 (jakarta) servlet chain. See {@link JavaxFilterBridge}.
 *
 * <p>The methods used by the hadoop-auth filter chain (headers, cookies,
 * method, URL/URI, remote address, attributes, scheme, body) delegate to the
 * wrapped jakarta request. Async, upgrade, multipart, session and dispatcher
 * operations are not part of that chain and throw
 * {@link UnsupportedOperationException}.
 */
public class JakartaToJavaxRequest implements HttpServletRequest {

  private final jakarta.servlet.http.HttpServletRequest delegate;

  public JakartaToJavaxRequest(jakarta.servlet.http.HttpServletRequest delegate) {
    this.delegate = delegate;
  }

  jakarta.servlet.http.HttpServletRequest getDelegate() {
    return delegate;
  }

  @Override
  public String getAuthType() {
    return delegate.getAuthType();
  }

  @Override
  public Cookie[] getCookies() {
    jakarta.servlet.http.Cookie[] source = delegate.getCookies();
    if (source == null) {
      return null;
    }
    Cookie[] result = new Cookie[source.length];
    for (int i = 0; i < source.length; i++) {
      result[i] = ServletBridgeUtils.toJavax(source[i]);
    }
    return result;
  }

  @Override
  public long getDateHeader(String name) {
    return delegate.getDateHeader(name);
  }

  @Override
  public String getHeader(String name) {
    return delegate.getHeader(name);
  }

  @Override
  public Enumeration<String> getHeaders(String name) {
    return delegate.getHeaders(name);
  }

  @Override
  public Enumeration<String> getHeaderNames() {
    return delegate.getHeaderNames();
  }

  @Override
  public int getIntHeader(String name) {
    return delegate.getIntHeader(name);
  }

  @Override
  public String getMethod() {
    return delegate.getMethod();
  }

  @Override
  public String getPathInfo() {
    return delegate.getPathInfo();
  }

  @Override
  public String getPathTranslated() {
    return delegate.getPathTranslated();
  }

  @Override
  public String getContextPath() {
    return delegate.getContextPath();
  }

  @Override
  public String getQueryString() {
    return delegate.getQueryString();
  }

  @Override
  public String getRemoteUser() {
    return delegate.getRemoteUser();
  }

  @Override
  public boolean isUserInRole(String role) {
    return delegate.isUserInRole(role);
  }

  @Override
  public Principal getUserPrincipal() {
    return delegate.getUserPrincipal();
  }

  @Override
  public String getRequestedSessionId() {
    return delegate.getRequestedSessionId();
  }

  @Override
  public String getRequestURI() {
    return delegate.getRequestURI();
  }

  @Override
  public StringBuffer getRequestURL() {
    return delegate.getRequestURL();
  }

  @Override
  public String getServletPath() {
    return delegate.getServletPath();
  }

  @Override
  public HttpSession getSession(boolean create) {
    throw new UnsupportedOperationException("getSession is not supported by the servlet bridge");
  }

  @Override
  public HttpSession getSession() {
    throw new UnsupportedOperationException("getSession is not supported by the servlet bridge");
  }

  @Override
  public String changeSessionId() {
    throw new UnsupportedOperationException("changeSessionId is not supported by the servlet bridge");
  }

  @Override
  public boolean isRequestedSessionIdValid() {
    return delegate.isRequestedSessionIdValid();
  }

  @Override
  public boolean isRequestedSessionIdFromCookie() {
    return delegate.isRequestedSessionIdFromCookie();
  }

  @Override
  public boolean isRequestedSessionIdFromURL() {
    return delegate.isRequestedSessionIdFromURL();
  }

  @Override
  @SuppressWarnings("deprecation")
  public boolean isRequestedSessionIdFromUrl() {
    return delegate.isRequestedSessionIdFromURL();
  }

  @Override
  public boolean authenticate(HttpServletResponse response) throws IOException, ServletException {
    throw new UnsupportedOperationException("authenticate is not supported by the servlet bridge");
  }

  @Override
  public void login(String username, String password) throws ServletException {
    throw new UnsupportedOperationException("login is not supported by the servlet bridge");
  }

  @Override
  public void logout() throws ServletException {
    throw new UnsupportedOperationException("logout is not supported by the servlet bridge");
  }

  @Override
  public Collection<Part> getParts() throws IOException, ServletException {
    throw new UnsupportedOperationException("getParts is not supported by the servlet bridge");
  }

  @Override
  public Part getPart(String name) throws IOException, ServletException {
    throw new UnsupportedOperationException("getPart is not supported by the servlet bridge");
  }

  @Override
  public <T extends HttpUpgradeHandler> T upgrade(Class<T> handlerClass) throws IOException, ServletException {
    throw new UnsupportedOperationException("upgrade is not supported by the servlet bridge");
  }

  @Override
  public Object getAttribute(String name) {
    return delegate.getAttribute(name);
  }

  @Override
  public Enumeration<String> getAttributeNames() {
    return delegate.getAttributeNames();
  }

  @Override
  public String getCharacterEncoding() {
    return delegate.getCharacterEncoding();
  }

  @Override
  public void setCharacterEncoding(String env) throws java.io.UnsupportedEncodingException {
    delegate.setCharacterEncoding(env);
  }

  @Override
  public int getContentLength() {
    return delegate.getContentLength();
  }

  @Override
  public long getContentLengthLong() {
    return delegate.getContentLengthLong();
  }

  @Override
  public String getContentType() {
    return delegate.getContentType();
  }

  @Override
  public ServletInputStream getInputStream() throws IOException {
    return new JakartaToJavaxInputStream(delegate.getInputStream());
  }

  @Override
  public String getParameter(String name) {
    return delegate.getParameter(name);
  }

  @Override
  public Enumeration<String> getParameterNames() {
    return delegate.getParameterNames();
  }

  @Override
  public String[] getParameterValues(String name) {
    return delegate.getParameterValues(name);
  }

  @Override
  public Map<String, String[]> getParameterMap() {
    return delegate.getParameterMap();
  }

  @Override
  public String getProtocol() {
    return delegate.getProtocol();
  }

  @Override
  public String getScheme() {
    return delegate.getScheme();
  }

  @Override
  public String getServerName() {
    return delegate.getServerName();
  }

  @Override
  public int getServerPort() {
    return delegate.getServerPort();
  }

  @Override
  public BufferedReader getReader() throws IOException {
    return delegate.getReader();
  }

  @Override
  public String getRemoteAddr() {
    return delegate.getRemoteAddr();
  }

  @Override
  public String getRemoteHost() {
    return delegate.getRemoteHost();
  }

  @Override
  public void setAttribute(String name, Object o) {
    delegate.setAttribute(name, o);
  }

  @Override
  public void removeAttribute(String name) {
    delegate.removeAttribute(name);
  }

  @Override
  public Locale getLocale() {
    return delegate.getLocale();
  }

  @Override
  public Enumeration<Locale> getLocales() {
    return delegate.getLocales();
  }

  @Override
  public boolean isSecure() {
    return delegate.isSecure();
  }

  @Override
  public RequestDispatcher getRequestDispatcher(String path) {
    throw new UnsupportedOperationException("getRequestDispatcher is not supported by the servlet bridge");
  }

  @Override
  @SuppressWarnings("deprecation")
  public String getRealPath(String path) {
    throw new UnsupportedOperationException("getRealPath is not supported by the servlet bridge");
  }

  @Override
  public int getRemotePort() {
    return delegate.getRemotePort();
  }

  @Override
  public String getLocalName() {
    return delegate.getLocalName();
  }

  @Override
  public String getLocalAddr() {
    return delegate.getLocalAddr();
  }

  @Override
  public int getLocalPort() {
    return delegate.getLocalPort();
  }

  @Override
  public ServletContext getServletContext() {
    throw new UnsupportedOperationException("getServletContext is not supported on the request bridge");
  }

  @Override
  public AsyncContext startAsync() {
    throw new UnsupportedOperationException("startAsync is not supported by the servlet bridge");
  }

  @Override
  public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse) {
    throw new UnsupportedOperationException("startAsync is not supported by the servlet bridge");
  }

  @Override
  public boolean isAsyncStarted() {
    return delegate.isAsyncStarted();
  }

  @Override
  public boolean isAsyncSupported() {
    return delegate.isAsyncSupported();
  }

  @Override
  public AsyncContext getAsyncContext() {
    throw new UnsupportedOperationException("getAsyncContext is not supported by the servlet bridge");
  }

  @Override
  public DispatcherType getDispatcherType() {
    return DispatcherType.valueOf(delegate.getDispatcherType().name());
  }
}
