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

package org.apache.hadoop.ozone.debug;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;

import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.cli.DebugSubcommand;
import org.apache.hadoop.hdds.utils.NativeLibraryLoader;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * CLI command to check if native libraries are loaded.
 */
@CommandLine.Command(name = "checknative",
    description = "Checks if native libraries are loaded")
@MetaInfServices(DebugSubcommand.class)
public class CheckNative extends AbstractSubcommand implements Callable<Void>, DebugSubcommand {

  @Override
  public Void call() throws Exception {
    boolean nativeHadoopLoaded = org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded();
    String hadoopLibraryName = "";
    String isalDetail = "";
    boolean isalLoaded = false;
    if (nativeHadoopLoaded) {
      hadoopLibraryName = org.apache.hadoop.util.NativeCodeLoader.getLibraryName();

      isalDetail = ErasureCodeNative.getLoadingFailureReason();
      if (isalDetail != null) {
        isalLoaded = false;
      } else {
        isalDetail = ErasureCodeNative.getLibraryName();
        isalLoaded = true;
      }
    }
    out().println("Native library checking:");
    out().printf("hadoop:  %b %s%n", nativeHadoopLoaded,
        hadoopLibraryName);
    out().printf("ISA-L:   %b %s%n", isalLoaded, isalDetail);

    // Attempt to load the rocks-tools lib
    boolean nativeRocksToolsLoaded = NativeLibraryLoader.getInstance().loadLibrary(
        ROCKS_TOOLS_NATIVE_LIBRARY_NAME,
        Collections.singletonList(ManagedRocksObjectUtils.getRocksDBLibFileName()));
    String rocksToolsDetail = "";
    if (nativeRocksToolsLoaded) {
      rocksToolsDetail = NativeLibraryLoader.getJniLibraryFileName();
    }
    out().printf("rocks-tools: %b %s%n", nativeRocksToolsLoaded, rocksToolsDetail);
    return null;
  }
}
