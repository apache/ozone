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
package org.apache.hadoop.ozone.shell.token;

import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import picocli.CommandLine;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Option for token file.
 */
public class TokenOption {

  @CommandLine.Option(names = {"--token", "-t"},
      description = "file containing encoded token",
      defaultValue = "/tmp/ozone.token",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private File tokenFile;

  public boolean exists() {
    boolean exists = tokenFile != null && tokenFile.exists();
    if (!exists) {
      System.err.println("Error: token operation failed as token file: "
          + tokenFile + " containing encoded token doesn't exist.");
    }
    return exists;
  }

  public Token<OzoneTokenIdentifier> decode() throws IOException {
    Credentials creds = new Credentials();
    try (FileInputStream fis = new FileInputStream(tokenFile)) {
      try (DataInputStream dis = new DataInputStream(fis)) {
        creds.readTokenStorageStream(dis);
      }
    }
    for (Token<? extends TokenIdentifier> token: creds.getAllTokens()) {
      if (token.getKind().equals(OzoneTokenIdentifier.KIND_NAME)) {
        return (Token<OzoneTokenIdentifier>) token;
      }
    }
    return null;
  }

  public void persistToken(Token<OzoneTokenIdentifier> token)
      throws IOException {
    try (FileOutputStream fos = new FileOutputStream(tokenFile)) {
      try (DataOutputStream dos = new DataOutputStream(fos)) {
        Credentials ts = new Credentials();
        ts.addToken(token.getService(), token);
        ts.writeTokenStorageToStream(dos);
        System.out.println("Token persisted to " + tokenFile.toString() +
            " successfully!");
      }
    }
  }
}