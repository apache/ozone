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
package org.apache.hadoop.hdds.utils;

import org.apache.hadoop.hdds.utils.db.managed.ManagedRawSSTFileReader;
import org.apache.ozone.test.tag.Native;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Matchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;
import static org.apache.hadoop.hdds.utils.NativeLibraryLoader.NATIVE_LIB_TMP_DIR;
import static org.mockito.Mockito.same;

/**
 * Test class for NativeLibraryLoader.
 */
public class TestNativeLibraryLoader {

  private static Stream<String> nativeLibraryDirectoryLocations()
      throws IOException {
    return Stream.of("", File.createTempFile("prefix", "suffix")
        .getParentFile().getAbsolutePath(), null);
  }

  @Native(ROCKS_TOOLS_NATIVE_LIBRARY_NAME)
  @ParameterizedTest
  @MethodSource("nativeLibraryDirectoryLocations")
  public void testNativeLibraryLoader(String nativeLibraryDirectoryLocation) throws NativeLibraryNotLoadedException {
    Map<String, Boolean> libraryLoadedMap = new HashMap<>();
    NativeLibraryLoader loader = new NativeLibraryLoader(libraryLoadedMap);
    try (MockedStatic<NativeLibraryLoader> mockedNativeLibraryLoader = Mockito.mockStatic(NativeLibraryLoader.class,
        Mockito.CALLS_REAL_METHODS)) {
      mockedNativeLibraryLoader.when(() -> NativeLibraryLoader.getSystemProperty(same(NATIVE_LIB_TMP_DIR)))
          .thenReturn(nativeLibraryDirectoryLocation);
      mockedNativeLibraryLoader.when(() -> NativeLibraryLoader.getInstance()).thenReturn(loader);
      ManagedRawSSTFileReader.loadLibrary();
      Assertions.assertTrue(NativeLibraryLoader.isLibraryLoaded(ROCKS_TOOLS_NATIVE_LIBRARY_NAME));
      // Mocking to force copy random bytes to create a lib file to
      // nativeLibraryDirectoryLocation. But load library will fail.
      mockedNativeLibraryLoader.when(() -> NativeLibraryLoader.getResourceStream(Matchers.anyString()))
          .thenReturn(new ByteArrayInputStream(new byte[]{0, 1, 2, 3}));
      String dummyLibraryName = "dummy_lib";
      NativeLibraryLoader.getInstance().loadLibrary(dummyLibraryName);
      NativeLibraryLoader.isLibraryLoaded(dummyLibraryName);
      // Checking if the resource with random was copied to a temp file.
      File[] libPath = new File(nativeLibraryDirectoryLocation == null ? "" : nativeLibraryDirectoryLocation)
          .getAbsoluteFile().listFiles((dir, name) -> name.startsWith(dummyLibraryName) &&
              name.endsWith(NativeLibraryLoader.getLibOsSuffix()));
      Assertions.assertNotNull(libPath);
      Assertions.assertEquals(1, libPath.length);
      Assertions.assertTrue(libPath[0].delete());
    }

  }
}
