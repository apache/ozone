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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.security.x509.keys;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

/**
 * Class for storing key material.
 */
public class KeyStorage {
  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  private static final Logger LOG = LoggerFactory.getLogger(KeyStorage.class);
  public static final Set<PosixFilePermission> DIR_PERMISSION_SET =
      ImmutableSet.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE);
  public static final Set<PosixFilePermission> FILE_PERMISSION_SET = ImmutableSet.of(OWNER_READ, OWNER_WRITE);
  private Path location;
  private SecurityConfig securityConfig;
  private BooleanSupplier isPosixFileSystem;
  private KeyCodec keyCodec;

  public KeyStorage(SecurityConfig config, String component) {
    securityConfig = config;
    isPosixFileSystem = KeyStorage::isPosix;
    this.location = securityConfig.getKeyLocation(component);
    keyCodec = new KeyCodec(config);
  }

  public KeyStorage(SecurityConfig config, Path keyDir) {
    this.securityConfig = config;
    this.location = keyDir;
    isPosixFileSystem = KeyStorage::isPosix;
    keyCodec = new KeyCodec(config);
    if (!location.toFile().exists()) {
      if (!location.toFile().mkdirs()) {
        throw new RuntimeException("Failed to create directory " + location);
      }
    }
  }

  private static boolean isPosix() {
    return FileSystems.getDefault().supportedFileAttributeViews()
        .contains("posix");
  }

  /**
   * Stores a given key using the default config options.
   *
   * @param keyPair - Key Pair to write to file.
   * @throws IOException - On I/O failure.
   */
  public void storeKey(KeyPair keyPair) throws IOException {
    storeKey(location, keyPair, securityConfig.getPrivateKeyFileName(),
        securityConfig.getPublicKeyFileName(), false);
  }


  /**
   * Stores a given private key using the default config options.
   *
   * @param key - Key to write to file.
   * @throws IOException - On I/O failure.
   */
  public void storePrivateKey(PrivateKey key) throws IOException {
    File privateKeyFile =
        Paths.get(location.toString(),
            securityConfig.getPrivateKeyFileName()).toFile();

    if (Files.exists(privateKeyFile.toPath())) {
      throw new IOException("Private key already exist.");
    }
    String encodedPrivateKey = keyCodec.encodePrivateKey(key);
    writeToFile(privateKeyFile, encodedPrivateKey);
    Files.setPosixFilePermissions(privateKeyFile.toPath(), FILE_PERMISSION_SET);
  }

  /**
   * Stores a given public key using the default config options.
   *
   * @param key - Key to write to file.
   * @throws IOException - On I/O failure.
   */
  public void storePublicKey(PublicKey key) throws IOException {
    File publicKeyFile = Paths.get(location.toString(),
        securityConfig.getPublicKeyFileName()).toFile();

    if (Files.exists(publicKeyFile.toPath())) {
      throw new IOException("Public key already exist.");
    }

    String encodedPublicKey = keyCodec.encodePublicKey(key);
    writeToFile(publicKeyFile, encodedPublicKey);
    Files.setPosixFilePermissions(publicKeyFile.toPath(), FILE_PERMISSION_SET);
  }

  /**
   * Stores a given keyPair using default config options.
   *
   * @param keyPair   - Key pair to write
   * @param overwrite - Overwrites the keys if they already exist.
   * @throws IOException - On I/O failure.
   */
  public void storeKey(KeyPair keyPair, boolean overwrite) throws IOException {
    storeKey(location, keyPair, securityConfig.getPrivateKeyFileName(),
        securityConfig.getPublicKeyFileName(), overwrite);
  }

  /**
   * Writes a given key using default config options.
   *
   * @param basePath  - The location to write to, override the config values.
   * @param keyPair   - Key pair to write
   * @param overwrite - Overwrites the keys if they already exist.
   * @throws IOException - On I/O failure.
   */
  public void storeKey(Path basePath, KeyPair keyPair, boolean overwrite)
      throws IOException {
    storeKey(basePath, keyPair, securityConfig.getPrivateKeyFileName(),
        securityConfig.getPublicKeyFileName(), overwrite);
  }

  private String readKeyFromFile(Path basePath, String keyFileName) throws IOException {
    File fileName = Paths.get(basePath.toString(), keyFileName).toFile();
    return FileUtils.readFileToString(fileName, DEFAULT_CHARSET);
  }

  /**
   * Returns a Private Key from an encoded file.
   *
   * @param basePath           - base path
   * @param privateKeyFileName - private key file name.
   * @return PrivateKey
   * @throws InvalidKeySpecException  - on Error.
   * @throws NoSuchAlgorithmException - on Error.
   * @throws IOException              - on Error.
   */
  public PrivateKey readPrivateKey(Path basePath, String privateKeyFileName)
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    String privateKeyEncoded = readKeyFromFile(basePath, privateKeyFileName);
    return keyCodec.decodePrivateKey(privateKeyEncoded);
  }

  /**
   * Read the Public Key using defaults.
   *
   * @return PublicKey.
   * @throws InvalidKeySpecException  - On Error.
   * @throws NoSuchAlgorithmException - On Error.
   * @throws IOException              - On Error.
   */
  public PublicKey readPublicKey() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    return readPublicKey(this.location.toAbsolutePath(),
        securityConfig.getPublicKeyFileName());
  }

  /**
   * Returns a public key from an encoded file.
   *
   * @param basePath          - base path.
   * @param publicKeyFileName - public key file name.
   * @return PublicKey
   * @throws NoSuchAlgorithmException - on Error.
   * @throws InvalidKeySpecException  - on Error.
   * @throws IOException              - on Error.
   */
  public PublicKey readPublicKey(Path basePath, String publicKeyFileName)
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    String publicKeyEncoded = readKeyFromFile(basePath, publicKeyFileName);
    return keyCodec.decodePublicKey(publicKeyEncoded);
  }


  /**
   * Returns the private key using defaults.
   *
   * @return PrivateKey.
   * @throws InvalidKeySpecException  - On Error.
   * @throws NoSuchAlgorithmException - On Error.
   * @throws IOException              - On Error.
   */
  public PrivateKey readPrivateKey() throws InvalidKeySpecException,
      NoSuchAlgorithmException, IOException {
    return readPrivateKey(this.location.toAbsolutePath(),
        securityConfig.getPrivateKeyFileName());
  }


  /**
   * Helper function that actually writes data to the files.
   *
   * @param basePath           - base path to write key
   * @param keyPair            - Key pair to write to file.
   * @param privateKeyFileName - private key file name.
   * @param publicKeyFileName  - public key file name.
   * @param force              - forces overwriting the keys.
   * @throws IOException - On I/O failure.
   */
  private synchronized void storeKey(Path basePath, KeyPair keyPair,
                                     String privateKeyFileName, String publicKeyFileName, boolean force)
      throws IOException {
    checkPreconditions(basePath);

    File privateKeyFile =
        Paths.get(basePath.toString(), privateKeyFileName).toFile();
    File publicKeyFile =
        Paths.get(basePath.toString(), publicKeyFileName).toFile();
    checkKeyFile(privateKeyFile, force, publicKeyFile);

    writeToFile(privateKeyFile, keyCodec.encodePrivateKey(keyPair.getPrivate()));
    writeToFile(publicKeyFile, keyCodec.encodePublicKey(keyPair.getPublic()));
    Files.setPosixFilePermissions(privateKeyFile.toPath(), FILE_PERMISSION_SET);
    Files.setPosixFilePermissions(publicKeyFile.toPath(), FILE_PERMISSION_SET);
  }

  private void writeToFile(File file, String material) throws IOException {
    try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
      fileOutputStream.write(material.getBytes(DEFAULT_CHARSET));
    }
  }


  /**
   * Checks if private and public key file already exists. Throws IOException if
   * file exists and force flag is set to false, else will delete the existing
   * file.
   *
   * @param privateKeyFile - Private key file.
   * @param force          - forces overwriting the keys.
   * @param publicKeyFile  - public key file.
   * @throws IOException - On I/O failure.
   */
  private void checkKeyFile(File privateKeyFile, boolean force,
                            File publicKeyFile) throws IOException {
    if (privateKeyFile.exists() && force) {
      if (!privateKeyFile.delete()) {
        throw new IOException("Unable to delete private key file.");
      }
    }

    if (publicKeyFile.exists() && force) {
      if (!publicKeyFile.delete()) {
        throw new IOException("Unable to delete public key file.");
      }
    }

    if (privateKeyFile.exists()) {
      throw new IOException("Private Key file already exists.");
    }

    if (publicKeyFile.exists()) {
      throw new IOException("Public Key file already exists.");
    }
  }

  /**
   * Checks if base path exists and sets file permissions.
   *
   * @param basePath - base path to write key
   * @throws IOException - On I/O failure.
   */
  private void checkPreconditions(Path basePath) throws IOException {
    Preconditions.checkNotNull(basePath, "Base path cannot be null");
    if (!isPosixFileSystem.getAsBoolean()) {
      LOG.error("Keys cannot be stored securely without POSIX file system "
          + "support for now.");
      throw new IOException("Unsupported File System.");
    }

    if (Files.exists(basePath)) {
      // Not the end of the world if we reset the permissions on an existing
      // directory.
      Files.setPosixFilePermissions(basePath, DIR_PERMISSION_SET);
    } else {
      boolean success = basePath.toFile().mkdirs();
      if (!success) {
        LOG.error("Unable to create the directory for the "
            + "location. Location: {}", basePath);
        throw new IOException("Unable to create the directory for the "
            + "location. Location:" + basePath);
      }
      Files.setPosixFilePermissions(basePath, DIR_PERMISSION_SET);
    }
  }

  /**
   * This function is used only for testing.
   *
   * @param isPosixFileSystem - Sets a boolean function for mimicking files
   *                          systems that are not posix.
   */
  @VisibleForTesting
  public void setIsPosixFileSystem(BooleanSupplier isPosixFileSystem) {
    this.isPosixFileSystem = isPosixFileSystem;
  }
}
