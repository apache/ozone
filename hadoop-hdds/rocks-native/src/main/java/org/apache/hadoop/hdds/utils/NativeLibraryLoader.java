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

package org.apache.hadoop.hdds.utils;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static String getJniLibraryFileName() {
    return appendLibOsSuffix("lib" + ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
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

  public static boolean isLibraryLoaded() {
    return isLibraryLoaded(ROCKS_TOOLS_NATIVE_LIBRARY_NAME);
  }

  public static boolean isLibraryLoaded(final String libraryName) {
    return getInstance().librariesLoaded.getOrDefault(libraryName, false);
  }

  public synchronized boolean loadLibrary(final String libraryName, final List<String> dependentFiles) {
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
        Pair<Optional<File>, List<File>> files = copyResourceFromJarToTemp(libraryName, dependentFiles);
        if (files.getKey().isPresent()) {
          System.load(files.getKey().get().getAbsolutePath());
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
  static InputStream getResourceStream(String libraryFileName) throws IOException {
    return NativeLibraryLoader.class.getClassLoader()
        .getResourceAsStream(libraryFileName);
  }

  private Pair<Optional<File>, List<File>> copyResourceFromJarToTemp(final String libraryName,
                                                                     final List<String> dependentFileNames)
      throws IOException {
    final String libraryFileName = getJniLibraryFileName(libraryName);
    InputStream is = null;
    try {
      is = getResourceStream(libraryFileName);
      if (is == null) {
        return Pair.of(Optional.empty(), null);
      }

      final String nativeLibDir =
          Objects.nonNull(getSystemProperty(NATIVE_LIB_TMP_DIR)) ?
              getSystemProperty(NATIVE_LIB_TMP_DIR) : "";
      final File dir = new File(nativeLibDir).getAbsoluteFile();

      // create a temporary dir to copy the library to
      final Path tempPath = Files.createTempDirectory(dir.toPath(), libraryName);
      final File tempDir = tempPath.toFile();
      if (!tempDir.exists()) {
        return Pair.of(Optional.empty(), null);
      }

      Path libPath = tempPath.resolve(libraryFileName);
      Files.copy(is, libPath, StandardCopyOption.REPLACE_EXISTING);
      File libFile = libPath.toFile();
      if (libFile.exists()) {
        libFile.deleteOnExit();
      }

      List<File> dependentFiles = new ArrayList<>();
      for (String fileName : dependentFileNames) {
        if (is != null) {
          is.close();
        }
        is = getResourceStream(fileName);
        Path path = tempPath.resolve(fileName);
        Files.copy(is, path, StandardCopyOption.REPLACE_EXISTING);
        File file = path.toFile();
        if (file.exists()) {
          file.deleteOnExit();
        }
        dependentFiles.add(file);
      }
      ShutdownHookManager.get().addShutdownHook(
          () -> FileUtils.deleteQuietly(tempDir),
          LIBRARY_SHUTDOWN_HOOK_PRIORITY);
      return Pair.of(Optional.of(libFile), dependentFiles);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }
}
