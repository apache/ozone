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

package org.apache.hadoop.hdds.security.x509.keys;

import static java.nio.file.Files.getPosixFilePermissions;
import static org.apache.hadoop.hdds.security.x509.keys.KeyStorage.DIR_PERMISSIONS;
import static org.apache.hadoop.hdds.security.x509.keys.KeyStorage.FILE_PERMISSIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.spi.FileSystemProvider;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Iterator;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * JUnit tests to run against the {@link KeyStorage} implementation.
 */
@DisplayName("Testing KeyStorage implementation")
@ExtendWith(MockitoExtension.class)
public class TestKeyStorage {

  @TempDir
  private Path baseDir;

  @Mock
  private SecurityConfig config;

  private static KeyPair keys;
  private static final String KEY_ALGO = "RSA";
  public static final String COMPONENT = "component";

  @BeforeAll
  public static void setupAKeyPair() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance(KEY_ALGO);
    keys = generator.generateKeyPair();
  }

  @BeforeEach
  public void setBaseConfigValues() throws Exception {
    when(config.getKeyLocation(anyString())).thenReturn(baseDir);
    when(config.keyCodec()).thenReturn(new KeyCodec(KEY_ALGO));
  }

  @Nested
  @DisplayName("with internal CA configured: ")
  class InternalCA {

    private static final String PRIVATE_KEY_FILENAME = "private-key.pem";
    private static final String PUBLIC_KEY_FILENAME = "public-key.pem";
    public static final String ERROR_MSG = "Fail.";

    @BeforeEach
    public void setConfigMockForInternalCA() {
      when(config.useExternalCACertificate(anyString())).thenReturn(false);
      when(config.getPrivateKeyFileName()).thenReturn(PRIVATE_KEY_FILENAME);
      when(config.getPublicKeyFileName()).thenReturn(PUBLIC_KEY_FILENAME);
    }

    @Test
    @DisplayName("store and read a key pair.")
    public void testStoreAndReadKeyPair() throws Exception {
      KeyStorage storage = new KeyStorage(config, COMPONENT);
      storeAndAssertDataWith(storage, baseDir);
    }

    @Test
    @DisplayName("store and read a key pair into a suffixed storage.")
    public void testStoreAndReadKeyPairWithSuffixedStorage() throws Exception {
      String pathSuffix = "keys";
      KeyStorage storage = new KeyStorage(config, COMPONENT, pathSuffix);
      Path expectedLocation = Paths.get(baseDir + pathSuffix);
      storeAndAssertDataWith(storage, expectedLocation);
    }

    @Test
    @DisplayName("attempt to store a key, fails during encoding and throws an IOException.")
    public void testStoreKeyFailToEncode() throws Exception {
      KeyCodec codec = mock(KeyCodec.class);
      when(codec.encodePrivateKey(any(PrivateKey.class))).thenThrow(new IOException(PRIVATE_KEY_FILENAME));
      when(codec.encodePublicKey(any(PublicKey.class))).thenThrow(new IOException(PUBLIC_KEY_FILENAME));

      when(config.keyCodec()).thenReturn(codec);
      KeyStorage storage = new KeyStorage(config, COMPONENT);

      IOException e = assertThrows(IOException.class, () -> storage.storePrivateKey(keys.getPrivate()));
      assertThat(e.getMessage()).isEqualTo(PRIVATE_KEY_FILENAME);
      e = assertThrows(IOException.class, () -> storage.storePublicKey(keys.getPublic()));
      assertThat(e.getMessage()).isEqualTo(PUBLIC_KEY_FILENAME);
    }

    @Test
    @DisplayName("attempt to store a key, fails during I/O operations and throws an IOException.")
    public void testStoreKeyFailToWrite() throws Exception {
      FileSystemProvider fsp = spy(FileSystemProvider.class);
      // this is needed for the file create to throw an exception
      when(fsp.newByteChannel(any(), any(), any())).thenThrow(new IOException(ERROR_MSG));
      // this is needed to avoid that the storage implementation sees the path as existing.
      doThrow(IOException.class).when(fsp).checkAccess(any(Path.class));
      // this is to avoid creating any directories for this test.
      doNothing().when(fsp).createDirectory(any(), any());

      FileSystem fs = mock(FileSystem.class);
      when(fs.provider()).thenReturn(fsp);

      Path p = mock(Path.class);
      when(p.getFileSystem()).thenReturn(fs);
      when(p.resolve(anyString())).thenReturn(p);

      when(config.getKeyLocation(anyString())).thenReturn(p);

      KeyStorage storage = new KeyStorage(config, COMPONENT);

      IOException e = assertThrows(IOException.class, () -> storage.storePrivateKey(keys.getPrivate()));
      assertThat(e.getMessage()).isEqualTo(ERROR_MSG);
      e = assertThrows(IOException.class, () -> storage.storePublicKey(keys.getPublic()));
      assertThat(e.getMessage()).isEqualTo(ERROR_MSG);
    }

    @Test
    @DisplayName("attempt to read a key, fails during decoding and throws an IOException.")
    public void testReadKeyFailToDecode() throws Exception {
      KeyCodec codec = spy(new KeyCodec(KEY_ALGO));
      doThrow(new IOException(PRIVATE_KEY_FILENAME)).when(codec).decodePrivateKey(any(byte[].class));
      doThrow(new IOException(PUBLIC_KEY_FILENAME)).when(codec).decodePublicKey(any(byte[].class));

      when(config.keyCodec()).thenReturn(codec);
      KeyStorage storage = new KeyStorage(config, COMPONENT);
      storage.storeKeyPair(keys);

      IOException e = assertThrows(IOException.class, storage::readPrivateKey);
      assertThat(e.getMessage()).isEqualTo(PRIVATE_KEY_FILENAME);
      e = assertThrows(IOException.class, storage::readPublicKey);
      assertThat(e.getMessage()).isEqualTo(PUBLIC_KEY_FILENAME);
    }

    @Test
    @DisplayName("attempt to read a key, fails during I/O operations and throws an IOException.")
    public void testReadKeyFailToWrite() throws Exception {
      FileSystemProvider fsp = spy(FileSystemProvider.class);
      // this is needed for the file create to throw an exception
      when(fsp.newByteChannel(any(), any(), any())).thenThrow(new IOException(ERROR_MSG));
      // this is needed to avoid that the storage implementation sees the path as existing.
      doThrow(IOException.class).when(fsp).checkAccess(any(Path.class));
      // this is to avoid creating any directories for this test.
      doNothing().when(fsp).createDirectory(any(), any());

      FileSystem fs = mock(FileSystem.class);
      when(fs.provider()).thenReturn(fsp);

      Path p = mock(Path.class);
      when(p.getFileSystem()).thenReturn(fs);
      when(p.resolve(anyString())).thenReturn(p);

      when(config.getKeyLocation(anyString())).thenReturn(p);

      KeyStorage storage = new KeyStorage(config, COMPONENT);

      IOException e = assertThrows(IOException.class, storage::readPrivateKey);
      assertThat(e.getMessage()).isEqualTo(ERROR_MSG);
      e = assertThrows(IOException.class, storage::readPublicKey);
      assertThat(e.getMessage()).isEqualTo(ERROR_MSG);
    }

    @Test
    @DisplayName("an attempt to overwrite an internal key throws FileAlreadyExists exception.")
    public void testInternalKeysAreNotOverWritable() throws Exception {
      KeyStorage storage = new KeyStorage(config, COMPONENT);
      storage.storePublicKey(keys.getPublic());
      storage.storePrivateKey(keys.getPrivate());
      assertThrows(FileAlreadyExistsException.class, () -> storage.storePublicKey(keys.getPublic()));
      assertThrows(FileAlreadyExistsException.class, () -> storage.storePrivateKey(keys.getPrivate()));
    }

    @Test
    @DisplayName("storage initialization fails because permissions can not be set.")
    @MockitoSettings(strictness = Strictness.LENIENT)
    public void testInitFailsOnPermissions() {
      // the mock will not return posix file attributes, so setting posix permissions fails.
      FileSystemProvider fsp = spy(FileSystemProvider.class);

      FileSystem fs = mock(FileSystem.class);
      when(fs.provider()).thenReturn(fsp);

      Path p = mock(Path.class);
      when(p.getFileSystem()).thenReturn(fs);
      when(p.resolve(anyString())).thenReturn(p);

      when(config.getKeyLocation(anyString())).thenReturn(p);

      assertThrows(UnsupportedOperationException.class, () -> new KeyStorage(config, COMPONENT));
    }

    @Test
    @DisplayName("storage initialization fails because directory creation fails.")
    @MockitoSettings(strictness = Strictness.LENIENT)
    public void testInitFailsOnDirCreation() throws Exception {
      // the mock will not return posix file attributes, so setting them fails.
      FileSystemProvider fsp = spy(FileSystemProvider.class);
      // first exception is to make path non-existent, second do nothing to get to the createDirectory call.
      doThrow(IOException.class).doNothing().when(fsp).checkAccess(any(Path.class));
      doThrow(new IOException(ERROR_MSG)).when(fsp).createDirectory(any(), any());

      FileSystem fs = mock(FileSystem.class);
      when(fs.provider()).thenReturn(fsp);

      // this is needed to get to the createDirectory call that we set to throw an exception
      Path pathMock = mock(Path.class);
      when(pathMock.getFileSystem()).thenReturn(fs);
      when(pathMock.resolve(any(Path.class))).thenReturn(pathMock);
      when(pathMock.toAbsolutePath()).thenReturn(pathMock);
      when(pathMock.getParent()).thenReturn(pathMock);
      when(pathMock.relativize(any(Path.class))).thenReturn(pathMock);

      Iterator pathIterMock = mock(Iterator.class);
      when(pathIterMock.hasNext()).thenReturn(true, false);
      when(pathIterMock.next()).thenReturn(pathMock);
      when(pathMock.iterator()).thenReturn(pathIterMock);

      when(config.getKeyLocation(anyString())).thenReturn(pathMock);

      IOException e = assertThrows(IOException.class, () -> new KeyStorage(config, COMPONENT));
      assertThat(e.getMessage()).isEqualTo(ERROR_MSG);
    }

    private void storeAndAssertDataWith(KeyStorage storage, Path expectedLocation) throws Exception {
      storage.storeKeyPair(keys);

      // Check if the files were written
      verify(config, times(1)).getKeyLocation(COMPONENT);

      Path privKeyPath = expectedLocation.resolve(PRIVATE_KEY_FILENAME);
      Path publicKeyPath = expectedLocation.resolve(PUBLIC_KEY_FILENAME);
      assertThat(privKeyPath).exists();
      assertThat(publicKeyPath).exists();
      assertThat(getPosixFilePermissions(privKeyPath)).containsExactlyInAnyOrderElementsOf(FILE_PERMISSIONS);
      assertThat(getPosixFilePermissions(publicKeyPath)).containsExactlyInAnyOrderElementsOf(FILE_PERMISSIONS);
      assertThat(getPosixFilePermissions(expectedLocation)).containsExactlyInAnyOrderElementsOf(DIR_PERMISSIONS);

      // Check if we can read the same keys from the files written
      KeyCodec codec = new KeyCodec(KEY_ALGO);

      PrivateKey privKey = codec.decodePrivateKey(Files.readAllBytes(privKeyPath));
      assertThat(privKey).isEqualTo(keys.getPrivate());

      PublicKey pubKey = codec.decodePublicKey(Files.readAllBytes(publicKeyPath));
      assertThat(pubKey).isEqualTo(keys.getPublic());

      // KeyPair does not implement equals, check keys read against the original keys.
      KeyPair kp = storage.readKeyPair();
      assertThat(kp.getPrivate()).isEqualTo(keys.getPrivate());
      assertThat(kp.getPublic()).isEqualTo(keys.getPublic());
    }
  }

  @Nested
  @DisplayName("with external CA configured: ")
  class ExternalCA {
    private Path externalPublicKeyPath;
    private Path externalPrivateKeyPath;

    @BeforeEach
    public void setConfigMockForExternalCA() {
      when(config.useExternalCACertificate(anyString())).thenReturn(true);
      when(config.getExternalRootCaPublicKeyPath()).thenReturn(externalPublicKeyPath);
      when(config.getExternalRootCaPrivateKeyPath()).thenReturn(externalPrivateKeyPath);
    }

    @BeforeEach
    public void addKeysToBaseDir() throws IOException {
      externalPublicKeyPath = baseDir.resolve("external-public-key.pem");
      externalPrivateKeyPath = baseDir.resolve("external-private-key.pem");
      Files.createFile(externalPublicKeyPath);
      Files.write(externalPublicKeyPath, config.keyCodec().encodePublicKey(keys.getPublic()));
      Files.createFile(externalPrivateKeyPath);
      Files.write(externalPrivateKeyPath, config.keyCodec().encodePrivateKey(keys.getPrivate()));
    }

    @Test
    @DisplayName("external CA keys are read.")
    public void testExternalKeysRead() throws Exception {
      KeyStorage storage = new KeyStorage(config, COMPONENT);

      assertThat(storage.readPublicKey()).isEqualTo(keys.getPublic());
      assertThat(storage.readPrivateKey()).isEqualTo(keys.getPrivate());
    }

    @Test
    @DisplayName("an attempt to overwrite an external key throws UnsupportedOperationException.")
    public void testExternalKeysAreNotOverWritable() throws Exception {
      KeyStorage storage = new KeyStorage(config, COMPONENT);

      assertThrows(UnsupportedOperationException.class, () -> storage.storePublicKey(keys.getPublic()));
      assertThrows(UnsupportedOperationException.class, () -> storage.storePrivateKey(keys.getPrivate()));
    }
  }
}
