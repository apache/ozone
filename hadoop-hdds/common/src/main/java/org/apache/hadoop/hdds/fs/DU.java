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
package org.apache.hadoop.hdds.fs;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Uses the unix 'du' program to calculate space usage.  Can be slow if there
 * are many files.
 *
 * @see SpaceUsageSource
 */
public class DU extends AbstractSpaceUsageSource {

  private static final Logger LOG = LoggerFactory.getLogger(DU.class);

  private final DUShell duShell;
  private final String[] command;
  private final String commandString;
  private final String excludePattern;

  public DU(File path) {
    this(path, null);
  }

  public DU(File path, String excludePattern) {
    super(path);

    this.excludePattern = excludePattern;
    command = constructCommand();
    commandString = String.join(" ", command);
    duShell = new DUShell();
  }

  @Override
  public long getUsedSpace() {
    return time(duShell::getUsed, LOG);
  }

  private String[] constructCommand() {
    List<String> parts = new LinkedList<>();
    parts.add("du");
    parts.add("-sk");
    if (excludePattern != null) {
      if (Shell.MAC) {
        parts.add("-I");
      } else {
        parts.add("--exclude");
      }
      parts.add(excludePattern);
    }
    parts.add(getPath());
    return parts.toArray(new String[0]);
  }

  private final class DUShell extends Shell {

    private final AtomicLong value = new AtomicLong();

    /**
     * @throws UncheckedIOException if shell command exited with error code
     */
    public long getUsed() {
      try {
        super.run();
        return value.get();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public String toString() {
      return commandString + "\n" + value.get() + "\t" + getPath();
    }

    @Override
    protected String[] getExecString() {
      return command;
    }

    @Override
    protected void parseExecResult(BufferedReader lines) throws IOException {
      String line = lines.readLine();
      if (line == null) {
        throw new IOException("Expecting a line not the end of stream");
      }

      String[] tokens = line.split("\t");
      if (tokens.length == 0) {
        throw new IOException("Illegal du output");
      }

      long kilobytes = Long.parseLong(tokens[0]);
      value.set(kilobytes * OzoneConsts.KB);
    }
  }

  @SuppressWarnings("squid:S106") // command-line program, output to stdout
  public static void main(String[] args) {
    String path = ".";
    if (args.length > 0) {
      path = args[0];
    }

    DU du = new DU(new File(path));
    du.duShell.getUsed();
    System.out.println(du.duShell);
  }
}
