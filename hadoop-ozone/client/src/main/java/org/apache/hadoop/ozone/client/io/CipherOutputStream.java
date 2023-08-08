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

package org.apache.hadoop.ozone.client.io;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NullCipher;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Copy of javax.crypto.CipherOutputStream with the method to return wrapped
 * output stream.
 */
public class CipherOutputStream extends FilterOutputStream {

  private Cipher cipher;
  private OutputStream output;
  private byte[] ibuffer = new byte[1];
  private byte[] obuffer;
  private boolean closed = false;

  public CipherOutputStream(OutputStream var1, Cipher var2) {
    super(var1);
    this.output = var1;
    this.cipher = var2;
  }

  protected CipherOutputStream(OutputStream var1) {
    super(var1);
    this.output = var1;
    this.cipher = new NullCipher();
  }

  public void write(int var1) throws IOException {
    this.ibuffer[0] = (byte)var1;
    this.obuffer = this.cipher.update(this.ibuffer, 0, 1);
    if (this.obuffer != null) {
      this.output.write(this.obuffer);
      this.obuffer = null;
    }

  }

  public void write(byte[] var1) throws IOException {
    this.write(var1, 0, var1.length);
  }

  public void write(byte[] var1, int var2, int var3) throws IOException {
    this.obuffer = this.cipher.update(var1, var2, var3);
    if (this.obuffer != null) {
      this.output.write(this.obuffer);
      this.obuffer = null;
    }

  }

  public void flush() throws IOException {
    if (this.obuffer != null) {
      this.output.write(this.obuffer);
      this.obuffer = null;
    }

    this.output.flush();
  }

  public void close() throws IOException {
    if (!this.closed) {
      this.closed = true;

      try {
        this.obuffer = this.cipher.doFinal();
      } catch (BadPaddingException | IllegalBlockSizeException var3) {
        this.obuffer = null;
      }

      try {
        this.flush();
      } catch (IOException var2) {
      }

      this.out.close();
    }
  }

  public OutputStream getWrappedStream() {
    return output;
  }

}
