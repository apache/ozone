/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import java.net.URI;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ozone.RootedOzoneFileSystem;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import org.kohsuke.MetaInfServices;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Tool that recover the lease of a specified file.
 */
@CommandLine.Command(
    name = "recover",
    customSynopsis = "ozone debug recover --path=<path>",
    description = "recover the lease of a specified file. Make sure to specify "
        + "file system scheme if ofs:// is not the default.")
@MetaInfServices(SubcommandWithParent.class)
public class LeaseRecoverer implements Callable<Void>, SubcommandWithParent {

  @CommandLine.ParentCommand
  private OzoneDebug parent;

  @Spec
  private CommandSpec spec;

  @CommandLine.Option(names = {"--path"},
      required = true,
      description = "Path to the file")
  private String path;

  public String getPath() {
    return path;
  }

  public void setPath(String dbPath) {
    this.path = dbPath;
  }

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  @Override
  public Void call() throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();
    URI uri = URI.create(this.path);
    FileSystem fs = FileSystem.get(uri, configuration);
    if (fs instanceof RootedOzoneFileSystem) {
      ((RootedOzoneFileSystem) fs).recoverLease(new Path(uri));
    } else {
      throw new IllegalArgumentException("Unsupported file system: "
          + fs.getScheme());
    }

    return null;
  }
}
