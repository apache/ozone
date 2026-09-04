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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Locale;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;

/**
 * Exposes a {@link jakarta.servlet.http.HttpServletResponse} as a
 * {@code javax.servlet.http.HttpServletResponse} so that hadoop's
 * {@code javax.servlet}-based authentication filters can write the response
 * (status, headers, cookies, error) inside a Jetty EE10 servlet chain.
 * See {@link JavaxFilterBridge}.
 */
public class JakartaToJavaxResponse implements HttpServletResponse {

  private final jakarta.servlet.http.HttpServletResponse delegate;

  public JakartaToJavaxResponse(jakarta.servlet.http.HttpServletResponse delegate) {
    this.delegate = delegate;
  }

  @Override
  public void addCookie(Cookie cookie) {
    delegate.addCookie(ServletBridgeUtils.toJakarta(cookie));
  }

  @Override
  public boolean containsHeader(String name) {
    return delegate.containsHeader(name);
  }

  @Override
  public String encodeURL(String url) {
    return delegate.encodeURL(url);
  }

  @Override
  public String encodeRedirectURL(String url) {
    return delegate.encodeRedirectURL(url);
  }

  @Override
  @SuppressWarnings("deprecation")
  public String encodeUrl(String url) {
    return delegate.encodeURL(url);
  }

  @Override
  @SuppressWarnings("deprecation")
  public String encodeRedirectUrl(String url) {
    return delegate.encodeRedirectURL(url);
  }

  @Override
  public void sendError(int sc, String msg) throws IOException {
    delegate.sendError(sc, msg);
  }

  @Override
  public void sendError(int sc) throws IOException {
    delegate.sendError(sc);
  }

  @Override
  public void sendRedirect(String location) throws IOException {
    delegate.sendRedirect(location);
  }

  @Override
  public void setDateHeader(String name, long date) {
    delegate.setDateHeader(name, date);
  }

  @Override
  public void addDateHeader(String name, long date) {
    delegate.addDateHeader(name, date);
  }

  @Override
  public void setHeader(String name, String value) {
    delegate.setHeader(name, value);
  }

  @Override
  public void addHeader(String name, String value) {
    delegate.addHeader(name, value);
  }

  @Override
  public void setIntHeader(String name, int value) {
    delegate.setIntHeader(name, value);
  }

  @Override
  public void addIntHeader(String name, int value) {
    delegate.addIntHeader(name, value);
  }

  @Override
  public void setStatus(int sc) {
    delegate.setStatus(sc);
  }

  @Override
  @SuppressWarnings("deprecation")
  public void setStatus(int sc, String sm) {
    // Servlet 6.0 removed the message form; the status code carries the meaning.
    delegate.setStatus(sc);
  }

  @Override
  public int getStatus() {
    return delegate.getStatus();
  }

  @Override
  public String getHeader(String name) {
    return delegate.getHeader(name);
  }

  @Override
  public Collection<String> getHeaders(String name) {
    return delegate.getHeaders(name);
  }

  @Override
  public Collection<String> getHeaderNames() {
    return delegate.getHeaderNames();
  }

  @Override
  public String getCharacterEncoding() {
    return delegate.getCharacterEncoding();
  }

  @Override
  public String getContentType() {
    return delegate.getContentType();
  }

  @Override
  public ServletOutputStream getOutputStream() throws IOException {
    return new JakartaToJavaxOutputStream(delegate.getOutputStream());
  }

  @Override
  public PrintWriter getWriter() throws IOException {
    return delegate.getWriter();
  }

  @Override
  public void setCharacterEncoding(String charset) {
    delegate.setCharacterEncoding(charset);
  }

  @Override
  public void setContentLength(int len) {
    delegate.setContentLength(len);
  }

  @Override
  public void setContentLengthLong(long len) {
    delegate.setContentLengthLong(len);
  }

  @Override
  public void setContentType(String type) {
    delegate.setContentType(type);
  }

  @Override
  public void setBufferSize(int size) {
    delegate.setBufferSize(size);
  }

  @Override
  public int getBufferSize() {
    return delegate.getBufferSize();
  }

  @Override
  public void flushBuffer() throws IOException {
    delegate.flushBuffer();
  }

  @Override
  public void resetBuffer() {
    delegate.resetBuffer();
  }

  @Override
  public boolean isCommitted() {
    return delegate.isCommitted();
  }

  @Override
  public void reset() {
    delegate.reset();
  }

  @Override
  public void setLocale(Locale loc) {
    delegate.setLocale(loc);
  }

  @Override
  public Locale getLocale() {
    return delegate.getLocale();
  }
}
