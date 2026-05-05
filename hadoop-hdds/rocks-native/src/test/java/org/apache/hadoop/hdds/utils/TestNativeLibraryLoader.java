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
import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_PROPERTY;
import static org.apache.hadoop.hdds.utils.NativeLibraryLoader.NATIVE_LIB_TMP_DIR;
import static org.apache.hadoop.hdds.utils.NativeLibraryLoader.getJniLibraryFileName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.same;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.utils.db.ManagedRawSSTFileReader;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

/**
 * Test class for NativeLibraryLoader.
 */
public class TestNativeLibraryLoader {

  @TempDir
  private static Path tempDir;

  private static Stream<String> nativeLibraryDirectoryLocations() {
    return Stream.of("", tempDir.toAbsolutePath().toString(), null);
  }

  @EnabledIfSystemProperty(named = ROCKS_TOOLS_NATIVE_PROPERTY, matches = "true")
  @ParameterizedTest
  @MethodSource("nativeLibraryDirectoryLocations")
  public void testNativeLibraryLoader(String nativeLibraryDirectoryLocation) throws NativeLibraryNotLoadedException {
    Map<String, Boolean> libraryLoadedMap = new HashMap<>();
    NativeLibraryLoader loader = new NativeLibraryLoader(libraryLoadedMap);
    try (MockedStatic<NativeLibraryLoader> mockedNativeLibraryLoader = mockStatic(NativeLibraryLoader.class,
        CALLS_REAL_METHODS)) {
      mockedNativeLibraryLoader.when(() -> NativeLibraryLoader.getSystemProperty(same(NATIVE_LIB_TMP_DIR)))
          .thenReturn(nativeLibraryDirectoryLocation);
      mockedNativeLibraryLoader.when(() -> NativeLibraryLoader.getInstance()).thenReturn(loader);
      ManagedRawSSTFileReader.loadLibrary();
      assertTrue(NativeLibraryLoader.isLibraryLoaded(ROCKS_TOOLS_NATIVE_LIBRARY_NAME));
    }
  }

  @ParameterizedTest
  @MethodSource("nativeLibraryDirectoryLocations")
  public void testDummyLibrary(String nativeLibraryDirectoryLocation) {
    Map<String, Boolean> libraryLoadedMap = new HashMap<>();
    NativeLibraryLoader loader = new NativeLibraryLoader(libraryLoadedMap);
    try (MockedStatic<NativeLibraryLoader> mockedNativeLibraryLoader = mockStatic(NativeLibraryLoader.class,
        CALLS_REAL_METHODS)) {
      mockedNativeLibraryLoader.when(() -> NativeLibraryLoader.getSystemProperty(same(NATIVE_LIB_TMP_DIR)))
          .thenReturn(nativeLibraryDirectoryLocation);
      mockedNativeLibraryLoader.when(NativeLibraryLoader::getInstance).thenReturn(loader);
      // Mocking to force copy random bytes to create a lib file to
      // nativeLibraryDirectoryLocation. But load library will fail.
      mockedNativeLibraryLoader.when(() -> NativeLibraryLoader.getResourceStream(anyString()))
          .thenReturn(new ByteArrayInputStream(new byte[]{0, 1, 2, 3}));
      String dummyLibraryName = "dummy_lib";
      List<String> dependencies = Arrays.asList("dep1", "dep2");
      File absDir = new File(nativeLibraryDirectoryLocation == null ? "" : nativeLibraryDirectoryLocation)
          .getAbsoluteFile();

      NativeLibraryLoader.getInstance().loadLibrary(dummyLibraryName, dependencies);

      // Checking if the resource with random was copied to a temp file.
      File[] libPath = absDir
          .listFiles((dir, name) -> name.startsWith(dummyLibraryName));
      assertThat(libPath)
          .isNotNull()
          .isNotEmpty();
      assertThat(libPath[0])
          .isDirectory();
      try {
        assertThat(new File(libPath[0], getJniLibraryFileName(dummyLibraryName)))
            .isFile();
        dependencies.forEach(dep -> assertThat(new File(libPath[0], dep)).isFile());
      } finally {
        FileUtils.deleteQuietly(libPath[0]);
      }
    }
  }
}
