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
import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;

/**
 * Exposes a {@link jakarta.servlet.ServletInputStream} as a
 * {@code javax.servlet.ServletInputStream}. Blocking reads delegate to the
 * wrapped stream; the non-blocking async read-listener API is not used by the
 * bridged auth filters and is unsupported.
 */
class JakartaToJavaxInputStream extends ServletInputStream {

  private final jakarta.servlet.ServletInputStream delegate;

  JakartaToJavaxInputStream(jakarta.servlet.ServletInputStream delegate) {
    this.delegate = delegate;
  }

  @Override
  public int read() throws IOException {
    return delegate.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return delegate.read(b, off, len);
  }

  @Override
  public int available() throws IOException {
    return delegate.available();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public boolean isFinished() {
    return delegate.isFinished();
  }

  @Override
  public boolean isReady() {
    return delegate.isReady();
  }

  @Override
  public void setReadListener(ReadListener readListener) {
    throw new UnsupportedOperationException("Async read listeners are not supported by the servlet bridge");
  }
}
