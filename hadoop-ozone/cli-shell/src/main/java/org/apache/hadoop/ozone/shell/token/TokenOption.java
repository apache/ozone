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

package org.apache.hadoop.ozone.shell.token;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import picocli.CommandLine;

/**
 * Option for token file.
 */
public class TokenOption {

  @CommandLine.Option(names = {"--token", "-t"},
      required = true,
      description = "file containing encoded token")
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
    try (InputStream fis = Files.newInputStream(tokenFile.toPath())) {
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
    try (OutputStream fos = Files.newOutputStream(tokenFile.toPath())) {
      try (DataOutputStream dos = new DataOutputStream(fos)) {
        Credentials ts = new Credentials();
        ts.addToken(token.getService(), token);
        ts.writeTokenStorageToStream(dos);
        System.out.println("Token persisted to " + tokenFile.toString() +
            " successfully!");
      }
    }
  }

  public String getTokenFilePath() {
    return tokenFile.getAbsolutePath();
  }
}
