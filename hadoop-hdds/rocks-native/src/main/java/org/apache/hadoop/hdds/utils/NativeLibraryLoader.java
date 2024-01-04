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

package org.apache.hadoop.hdds.utils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class to load Native Libraries.
 */
public class NativeLibraryLoader {

  private static final Logger LOG =
      LoggerFactory.getLogger(NativeLibraryLoader.class);
  public static final int LIBRARY_SHUTDOWN_HOOK_PRIORITY = 1;
  private static final String OS = System.getProperty("os.name").toLowerCase();

  public static final String NATIVE_LIB_TMP_DIR = "native.lib.tmp.dir";
  private Map<String, Boolean> librariesLoaded;
  private static volatile NativeLibraryLoader instance;

  public NativeLibraryLoader(final Map<String, Boolean> librariesLoaded) {
    this.librariesLoaded = librariesLoaded;
  }

  private static synchronized void initNewInstance() {
    if (instance == null) {
      instance = new NativeLibraryLoader(new ConcurrentHashMap<>());
    }
  }

  public static NativeLibraryLoader getInstance() {
    if (instance == null) {
      initNewInstance();
    }
    return instance;
  }

  public static String getJniLibraryFileName(String libraryName) {
    return appendLibOsSuffix("lib" + libraryName);
  }

  public static boolean isMac() {
    return OS.startsWith("mac");
  }

  public static boolean isWindows() {
    return OS.startsWith("win");
  }

  public static boolean isLinux() {
    return OS.startsWith("linux");
  }

  @VisibleForTesting
  static String getLibOsSuffix() {
    if (isMac()) {
      return ".dylib";
    } else if (isWindows()) {
      return ".dll";
    } else if (isLinux()) {
      return ".so";
    }
    throw new UnsatisfiedLinkError(String.format("Unsupported OS %s", OS));
  }

  private static String appendLibOsSuffix(String libraryFileName) {
    return libraryFileName + getLibOsSuffix();
  }

  public static boolean isLibraryLoaded(final String libraryName) {
    return getInstance().librariesLoaded
        .getOrDefault(libraryName, false);
  }

  public synchronized boolean loadLibrary(final String libraryName) {
    if (isLibraryLoaded(libraryName)) {
      return true;
    }
    LOG.info("Loading Library: {}", libraryName);
    boolean loaded = false;
    try {
      loaded = false;
      try {
        System.loadLibrary(libraryName);
        loaded = true;
      } catch (Throwable e) {

      }
      if (!loaded) {
        Optional<File> file = copyResourceFromJarToTemp(libraryName);
        if (file.isPresent()) {
          System.load(file.get().getAbsolutePath());
          loaded = true;
        }
      }
    } catch (Throwable e) {
      LOG.warn("Unable to load library: {}", libraryName, e);
    }
    this.librariesLoaded.put(libraryName, loaded);
    return isLibraryLoaded(libraryName);
  }

  // Added function to make this testable.
  @VisibleForTesting
  static String getSystemProperty(String property) {
    return System.getProperty(property);
  }

  // Added function to make this testable
  @VisibleForTesting
  static InputStream getResourceStream(String libraryFileName) {
    return NativeLibraryLoader.class.getClassLoader()
        .getResourceAsStream(libraryFileName);
  }

  private Optional<File> copyResourceFromJarToTemp(final String libraryName)
      throws IOException {
    final String libraryFileName = getJniLibraryFileName(libraryName);
    InputStream is = null;
    try {
      is = getResourceStream(libraryFileName);
      if (is == null) {
        return Optional.empty();
      }

      final String nativeLibDir =
          Objects.nonNull(getSystemProperty(NATIVE_LIB_TMP_DIR)) ?
              getSystemProperty(NATIVE_LIB_TMP_DIR) : "";
      final File dir = new File(nativeLibDir).getAbsoluteFile();

      // create a temporary file to copy the library to
      final File temp = File.createTempFile(libraryName, getLibOsSuffix(), dir);
      if (!temp.exists()) {
        return Optional.empty();
      } else {
        temp.deleteOnExit();
      }

      Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      ShutdownHookManager.get().addShutdownHook(temp::delete,
          LIBRARY_SHUTDOWN_HOOK_PRIORITY);
      return Optional.of(temp);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }
}
