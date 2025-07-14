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

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.nio.file.attribute.PosixFilePermissions.fromString;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Set;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The KeyStorage is responsible to persist and read an RSA keypair for an Ozone component in PEM format.<br/>
 * <p>
 * Ozone component in this sense means not just DN, SCM or OM as it originally did, as rotation overloaded the
 * component to a component-stage pair to control the location during different stages of the certificate rotation.
 * </p>
 * <p>
 * In the general case, the storage used is defined by the {@link SecurityConfig} of the system. It resolves the
 * configuration, and provides the private/public key's basedir based on the name of the component using this object.
 * If the path specified is not on a POSIX compliant file system, the operations will fail, as this class ensures that
 * only the owner can read/write the directory, and only the owner can read/write the key files with the help of POSIX
 * file permissions.
 * </p>
 * There are some special cases:<br/>
 * <p>
 * One is when the Root CA certificate and its keys are supplied externally to Ozone, and the
 * component using this KeyStorage is {@link org.apache.hadoop.ozone.OzoneConsts#SCM_ROOT_CA_COMPONENT_NAME} in which
 * case the {@link SecurityConfig#useExternalCACertificate(String)} returns true. In this environment, this object
 * loads the keys from the specified paths in the configuration.
 * Note that the configuration may not contain a public key path in this setup, in which case the CAServer code reads
 * the public key from the certificate, as the storage would throw a {@link java.io.FileNotFoundException} on an attempt
 * to read the public key from an empty path.
 * </p>
 * <p>
 * An other is during rotation, where to ensure that we atomically switch keys and certificates, the path of the newly
 * generated keys are defined either by changing the component name, or by suffixing the whole path that ends with the
 * keys directory. (See {@link SecurityConfig#getKeyLocation(String)} and its usage in RootCARotationManager and in the
 * DefaultCertificateClient.
 * For the case where the component is changing, it is straightforward to just use the changed component name, for the
 * where the keys folder is suffixed, this class provides a constructor to define the suffix.
 * </p>
 *
 * @see SecurityConfig#getKeyLocation(String) to understand how the location is resolved for a component
 * @see SecurityConfig#useExternalCACertificate(String) to understand the decision on using an external key pair
 * @see SecurityConfig#getKeyAlgo()
 */
// Also see:
// DefaultCertificateClient on rotation tasks
// RootCARotationManager on the root CA certificate rotation tasks
public class KeyStorage {
  private static final Logger LOG = LoggerFactory.getLogger(KeyStorage.class);

  public static final Set<PosixFilePermission> DIR_PERMISSIONS = fromString("rwx------");
  public static final Set<PosixFilePermission> FILE_PERMISSIONS = fromString("rw-------");

  private final Path privateKeyPath;
  private final Path publicKeyPath;
  private final KeyCodec keyCodec;
  private final boolean externalKeysUsed;

  /**
   * Creates a KeyStorage object based on the configuration for the defined component, by assuming an empty key path
   * suffix.
   *
   * @param config the SecurityConfiguration of the application
   * @param component the component for which the KeyStorage is to be created
   * @throws IOException in case the defined paths are unusable, or if the key algorithm in the configuration is
   *                     unsupported.
   * @see KeyStorage#KeyStorage(SecurityConfig, String, String) for more details.
   */
  public KeyStorage(SecurityConfig config, String component) throws IOException {
    this(config, component, config.getKeyLocation(component));
  }

  /**
   * Creates a KeyStorage object based on the configuration, the defined component, suffixing the base path with the
   * defined suffix.<br/><br/>
   * <p>
   * The initialization is retrieving the base path from {@link SecurityConfig#getKeyLocation(String)} method,
   * within which {@link SecurityConfig#getPrivateKeyFileName()} and {@link SecurityConfig#getPublicKeyFileName()}
   * defined the name of the files that holds the public and private keys respectively.<br/>
   * The base path is suffixed with the keyDirSuffix before resolving the path belongs to the key files in the folder.
   * If the base path does not exists, it is created with "rwx------" POSIX permissions, if it exists, the code attempts
   * to set its POSIX permissions to "rwx------". Key files are created with "rw-------" POSIX permission bits set.
   * </p><br/>
   * <p>
   * In case {@link SecurityConfig#useExternalCACertificate(String)} returns true, the public and private key is read
   * from {@link SecurityConfig#getExternalRootCaPublicKeyPath()} and
   * {@link SecurityConfig#getExternalRootCaPrivateKeyPath()} respectively.
   * If {@link SecurityConfig#useExternalCACertificate(String)} is true at the time of constructing this object, the
   * store methods of the instance are not usable, and will throw an {@link UnsupportedOperationException}.
   * </p>
   * @param config the {@link SecurityConfig} of the system
   * @param component the component name to be used
   * @param keyDirSuffix the suffix to apply to the keys base path
   * @throws IOException in case when base directory can not be created with the desired permissions, or if the key
   *                     algorithm is not available for the {@link java.security.KeyFactory}
   */
  public KeyStorage(SecurityConfig config, String component, String keyDirSuffix) throws IOException {
    this(config, component, Paths.get(config.getKeyLocation(component).toString() + keyDirSuffix));
  }

  private KeyStorage(SecurityConfig config, String component, Path keyPath) throws IOException {
    if (config.useExternalCACertificate(component)) {
      privateKeyPath = config.getExternalRootCaPrivateKeyPath();
      if (!Files.isReadable(privateKeyPath)) {
        throw new UnsupportedEncodingException("External private key path is not readable: " + privateKeyPath);
      }
      publicKeyPath = config.getExternalRootCaPublicKeyPath();
      if (!Files.isReadable(publicKeyPath)) {
        throw new UnsupportedEncodingException("External public key path is not readable: " + publicKeyPath);
      }
      externalKeysUsed = true;
    } else {
      createOrSanitizeDirectory(keyPath);
      privateKeyPath = keyPath.resolve(config.getPrivateKeyFileName());
      publicKeyPath = keyPath.resolve(config.getPublicKeyFileName());
      externalKeysUsed = false;
    }

    keyCodec = config.keyCodec();
  }

  /**
   * Returns the private key stored in the private key file.
   *
   * @return PrivateKey the key read from the private key file.
   * @throws IOException in case the file is unreadable, or decoding the contents from PEM format fails.
   */
  public PrivateKey readPrivateKey() throws IOException {
    LOG.info("Reading private key from {}.", privateKeyPath);
    try {
      return keyCodec.decodePrivateKey(readAllBytes(privateKeyPath));
    } catch (IOException e) {
      LOG.error("Failed to read the private key.", e);
      throw e;
    }
  }

  /**
   * Returns a public key from an encoded file.
   *
   * @return PublicKey
   * @throws IOException              - on Error.
   */
  public PublicKey readPublicKey() throws IOException {
    LOG.info("Reading public key from {}.", publicKeyPath);
    try {
      return keyCodec.decodePublicKey(readAllBytes(publicKeyPath));
    } catch (IOException e) {
      LOG.error("Failed to read the public key.", e);
      throw e;
    }
  }

  public KeyPair readKeyPair() throws IOException {
    return new KeyPair(readPublicKey(), readPrivateKey());
  }

  /**
   * Stores a given private key using the default config options.
   *
   * @param key - Key to write to file.
   * @throws IOException - On I/O failure.
   */
  public void storePrivateKey(PrivateKey key) throws IOException {
    LOG.info("Storing private key to {}.", privateKeyPath);
    try {
      storeKey(privateKeyPath, keyCodec.encodePrivateKey(key));
    } catch (IOException e) {
      LOG.error("Failed to persist the private key.", e);
      throw e;
    }
  }

  /**
   * Stores a given public key using the default config options.
   *
   * @param key - Key to write to file.
   * @throws IOException - On I/O failure.
   */
  public void storePublicKey(PublicKey key) throws IOException {
    LOG.info("Storing public key to {}.", publicKeyPath);
    try {
      storeKey(publicKeyPath, keyCodec.encodePublicKey(key));
    } catch (IOException e) {
      LOG.error("Failed to persist the public key.", e);
      throw e;
    }
  }

  /**
   * Helper function that actually writes data to the files.
   *
   * @param keyPair - Key pair to write to file.
   * @throws IOException - On I/O failure.
   */
  public void storeKeyPair(KeyPair keyPair) throws IOException {
    storePublicKey(keyPair.getPublic());
    storePrivateKey(keyPair.getPrivate());
  }

  private void storeKey(Path keyPath, byte[] encodedKey) throws IOException {
    if (externalKeysUsed) {
      throw new UnsupportedOperationException("Attempt to override external keys.");
    }
    Files.createFile(keyPath, asFileAttribute(FILE_PERMISSIONS));
    Files.write(keyPath, encodedKey);
  }

  private void createOrSanitizeDirectory(Path dir) throws IOException {
    if (Files.exists(dir)) {
      // Sanity reset of permissions.
      Files.setPosixFilePermissions(dir, DIR_PERMISSIONS);
    } else {
      Files.createDirectories(dir, asFileAttribute(DIR_PERMISSIONS));
    }
  }
}
