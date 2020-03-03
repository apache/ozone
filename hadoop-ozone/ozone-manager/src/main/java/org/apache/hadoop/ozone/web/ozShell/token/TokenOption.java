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
package org.apache.hadoop.ozone.web.ozShell.token;

import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * Option for token file.
 */
public class TokenOption {

  @CommandLine.Option(names = {"--token", "-t"},
      description = "file containing encoded token",
      defaultValue = "/tmp/token.txt",
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
    Token<OzoneTokenIdentifier> token = new Token<>();
    token.decodeFromUrlString(new String(Files.readAllBytes(tokenFile.toPath()),
        StandardCharsets.UTF_8));
    return token;
  }

}