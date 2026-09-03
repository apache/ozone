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

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.EventListener;
import java.util.Map;
import java.util.Set;
import javax.servlet.Filter;
import javax.servlet.FilterRegistration;
import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;
import javax.servlet.SessionCookieConfig;
import javax.servlet.SessionTrackingMode;
import javax.servlet.descriptor.JspConfigDescriptor;

/**
 * Exposes a {@link jakarta.servlet.ServletContext} as a
 * {@code javax.servlet.ServletContext}. Only the operations used by hadoop's
 * authentication filter chain and secret-provider construction are delegated
 * (attributes, init parameters, logging, resources, version). Attributes are
 * read from and written to the underlying jakarta context, so a value set on
 * the real context (for example the signer secret provider) is visible through
 * this view. Servlet/filter registration, dispatchers, listeners, session
 * configuration and JSP descriptors are not part of that chain and throw
 * {@link UnsupportedOperationException}.
 */
public class JakartaToJavaxServletContext implements ServletContext {

  private final jakarta.servlet.ServletContext delegate;

  public JakartaToJavaxServletContext(jakarta.servlet.ServletContext delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getContextPath() {
    return delegate.getContextPath();
  }

  @Override
  public ServletContext getContext(String uripath) {
    throw new UnsupportedOperationException("getContext is not supported by the servlet bridge");
  }

  @Override
  public int getMajorVersion() {
    return delegate.getMajorVersion();
  }

  @Override
  public int getMinorVersion() {
    return delegate.getMinorVersion();
  }

  @Override
  public int getEffectiveMajorVersion() {
    return delegate.getEffectiveMajorVersion();
  }

  @Override
  public int getEffectiveMinorVersion() {
    return delegate.getEffectiveMinorVersion();
  }

  @Override
  public String getMimeType(String file) {
    return delegate.getMimeType(file);
  }

  @Override
  public Set<String> getResourcePaths(String path) {
    return delegate.getResourcePaths(path);
  }

  @Override
  public URL getResource(String path) throws MalformedURLException {
    return delegate.getResource(path);
  }

  @Override
  public InputStream getResourceAsStream(String path) {
    return delegate.getResourceAsStream(path);
  }

  @Override
  public RequestDispatcher getRequestDispatcher(String path) {
    throw new UnsupportedOperationException("getRequestDispatcher is not supported by the servlet bridge");
  }

  @Override
  public RequestDispatcher getNamedDispatcher(String name) {
    throw new UnsupportedOperationException("getNamedDispatcher is not supported by the servlet bridge");
  }

  @Override
  @SuppressWarnings("deprecation")
  public Servlet getServlet(String name) throws ServletException {
    return null;
  }

  @Override
  @SuppressWarnings("deprecation")
  public Enumeration<Servlet> getServlets() {
    return Collections.emptyEnumeration();
  }

  @Override
  @SuppressWarnings("deprecation")
  public Enumeration<String> getServletNames() {
    return Collections.emptyEnumeration();
  }

  @Override
  public void log(String msg) {
    delegate.log(msg);
  }

  @Override
  @SuppressWarnings("deprecation")
  public void log(Exception exception, String msg) {
    delegate.log(msg, exception);
  }

  @Override
  public void log(String message, Throwable throwable) {
    delegate.log(message, throwable);
  }

  @Override
  public String getRealPath(String path) {
    return delegate.getRealPath(path);
  }

  @Override
  public String getServerInfo() {
    return delegate.getServerInfo();
  }

  @Override
  public String getInitParameter(String name) {
    return delegate.getInitParameter(name);
  }

  @Override
  public Enumeration<String> getInitParameterNames() {
    return delegate.getInitParameterNames();
  }

  @Override
  public boolean setInitParameter(String name, String value) {
    return delegate.setInitParameter(name, value);
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
  public void setAttribute(String name, Object object) {
    delegate.setAttribute(name, object);
  }

  @Override
  public void removeAttribute(String name) {
    delegate.removeAttribute(name);
  }

  @Override
  public String getServletContextName() {
    return delegate.getServletContextName();
  }

  @Override
  public ServletRegistration.Dynamic addServlet(String servletName, String className) {
    throw new UnsupportedOperationException("addServlet is not supported by the servlet bridge");
  }

  @Override
  public ServletRegistration.Dynamic addServlet(String servletName, Servlet servlet) {
    throw new UnsupportedOperationException("addServlet is not supported by the servlet bridge");
  }

  @Override
  public ServletRegistration.Dynamic addServlet(String servletName, Class<? extends Servlet> servletClass) {
    throw new UnsupportedOperationException("addServlet is not supported by the servlet bridge");
  }

  @Override
  public <T extends Servlet> T createServlet(Class<T> clazz) throws ServletException {
    throw new UnsupportedOperationException("createServlet is not supported by the servlet bridge");
  }

  @Override
  public ServletRegistration getServletRegistration(String servletName) {
    throw new UnsupportedOperationException("getServletRegistration is not supported by the servlet bridge");
  }

  @Override
  public Map<String, ? extends ServletRegistration> getServletRegistrations() {
    throw new UnsupportedOperationException("getServletRegistrations is not supported by the servlet bridge");
  }

  @Override
  public FilterRegistration.Dynamic addFilter(String filterName, String className) {
    throw new UnsupportedOperationException("addFilter is not supported by the servlet bridge");
  }

  @Override
  public FilterRegistration.Dynamic addFilter(String filterName, Filter filter) {
    throw new UnsupportedOperationException("addFilter is not supported by the servlet bridge");
  }

  @Override
  public FilterRegistration.Dynamic addFilter(String filterName, Class<? extends Filter> filterClass) {
    throw new UnsupportedOperationException("addFilter is not supported by the servlet bridge");
  }

  @Override
  public <T extends Filter> T createFilter(Class<T> clazz) throws ServletException {
    throw new UnsupportedOperationException("createFilter is not supported by the servlet bridge");
  }

  @Override
  public FilterRegistration getFilterRegistration(String filterName) {
    throw new UnsupportedOperationException("getFilterRegistration is not supported by the servlet bridge");
  }

  @Override
  public Map<String, ? extends FilterRegistration> getFilterRegistrations() {
    throw new UnsupportedOperationException("getFilterRegistrations is not supported by the servlet bridge");
  }

  @Override
  public SessionCookieConfig getSessionCookieConfig() {
    throw new UnsupportedOperationException("getSessionCookieConfig is not supported by the servlet bridge");
  }

  @Override
  public void setSessionTrackingModes(Set<SessionTrackingMode> sessionTrackingModes) {
    throw new UnsupportedOperationException("setSessionTrackingModes is not supported by the servlet bridge");
  }

  @Override
  public Set<SessionTrackingMode> getDefaultSessionTrackingModes() {
    throw new UnsupportedOperationException("getDefaultSessionTrackingModes is not supported by the servlet bridge");
  }

  @Override
  public Set<SessionTrackingMode> getEffectiveSessionTrackingModes() {
    throw new UnsupportedOperationException("getEffectiveSessionTrackingModes is not supported by the servlet bridge");
  }

  @Override
  public void addListener(String className) {
    throw new UnsupportedOperationException("addListener is not supported by the servlet bridge");
  }

  @Override
  public <T extends EventListener> void addListener(T t) {
    throw new UnsupportedOperationException("addListener is not supported by the servlet bridge");
  }

  @Override
  public void addListener(Class<? extends EventListener> listenerClass) {
    throw new UnsupportedOperationException("addListener is not supported by the servlet bridge");
  }

  @Override
  public <T extends EventListener> T createListener(Class<T> clazz) throws ServletException {
    throw new UnsupportedOperationException("createListener is not supported by the servlet bridge");
  }

  @Override
  public JspConfigDescriptor getJspConfigDescriptor() {
    throw new UnsupportedOperationException("getJspConfigDescriptor is not supported by the servlet bridge");
  }

  @Override
  public ClassLoader getClassLoader() {
    return delegate.getClassLoader();
  }

  @Override
  public void declareRoles(String... roleNames) {
    throw new UnsupportedOperationException("declareRoles is not supported by the servlet bridge");
  }

  @Override
  public String getVirtualServerName() {
    return delegate.getVirtualServerName();
  }
}
