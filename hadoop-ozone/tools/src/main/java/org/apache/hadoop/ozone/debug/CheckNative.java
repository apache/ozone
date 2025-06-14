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
import java.util.function.Supplier;
import org.apache.hadoop.crypto.OpensslCipher;
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

  private static class LibraryCheckResult {
    private final boolean loaded;
    private final String detail;

    LibraryCheckResult(boolean loaded, String detail) {
      this.loaded = loaded;
      this.detail = detail;
    }

    public boolean isLoaded() {
      return loaded;
    }

    public String getDetail() {
      return detail;
    }
  }

  private LibraryCheckResult getLibraryStatus(
      Supplier<String> failureReasonSupplier,
      Supplier<String> libraryNameSupplier) {
    String failureReason = failureReasonSupplier.get();
    if (failureReason != null) {
      return new LibraryCheckResult(false, failureReason);
    } else {
      return new LibraryCheckResult(true, libraryNameSupplier.get());
    }
  }

  @Override
  public Void call() throws Exception {
    boolean nativeHadoopLoaded = org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded();
    String hadoopLibraryName = "";
    String isalDetail = "";
    boolean isalLoaded = false;
    String opensslDetail = "";
    boolean opensslLoaded = false;

    if (nativeHadoopLoaded) {
      hadoopLibraryName = org.apache.hadoop.util.NativeCodeLoader.getLibraryName();

      LibraryCheckResult isalStatus = getLibraryStatus(
          ErasureCodeNative::getLoadingFailureReason,
          ErasureCodeNative::getLibraryName
      );
      isalLoaded = isalStatus.isLoaded();
      isalDetail = isalStatus.getDetail();

      // Check OpenSSL status
      LibraryCheckResult opensslStatus = getLibraryStatus(
          OpensslCipher::getLoadingFailureReason,
          OpensslCipher::getLibraryName
      );
      opensslLoaded = opensslStatus.isLoaded();
      opensslDetail = opensslStatus.getDetail();
    }
    out().println("Native library checking:");
    out().printf("hadoop:  %b %s%n", nativeHadoopLoaded,
        hadoopLibraryName);
    out().printf("ISA-L:   %b %s%n", isalLoaded, isalDetail);
    out().printf("OpenSSL: %b %s%n", opensslLoaded, opensslDetail);

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
