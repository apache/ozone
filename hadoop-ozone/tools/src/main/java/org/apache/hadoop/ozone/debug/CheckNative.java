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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.apache.hadoop.crypto.OpensslCipher;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.cli.DebugSubcommand;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.util.NativeCodeLoader;
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
    Map<String, Object> results = new LinkedHashMap<>();

    // Hadoop
    results.put("hadoop", checkLibrary(NativeCodeLoader.isNativeCodeLoaded(), NativeCodeLoader::getLibraryName));
    results.put("ISA-L", checkLibrary(ErasureCodeNative.getLoadingFailureReason(), ErasureCodeNative::getLibraryName));
    results.put("OpenSSL", checkLibrary(
        // OpensslCipher provides cryptic reason if Hadoop native library itself is not loaded
        NativeCodeLoader.isNativeCodeLoaded() ? OpensslCipher.getLoadingFailureReason() : "",
        OpensslCipher::getLibraryName
    ));

    final int maxLength = results.keySet().stream()
        .mapToInt(String::length)
        .max()
        .getAsInt();

    out().println("Native library checking:");
    results.forEach((name, result) -> out().printf("%" + maxLength + "s:  %s%n", name, result));

    return null;
  }

  private static Object checkLibrary(boolean loaded, Supplier<String> libraryName) {
    return checkLibrary(loaded ? null : "", libraryName);
  }

  private static Object checkLibrary(String failureReason, Supplier<String> libraryName) {
    boolean loaded = failureReason == null;
    String detail = loaded ? libraryName.get() : failureReason;
    return String.format("%-5b  %s", loaded, detail);
  }
}
