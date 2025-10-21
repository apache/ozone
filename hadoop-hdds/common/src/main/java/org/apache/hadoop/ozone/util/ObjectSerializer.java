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

package org.apache.hadoop.ozone.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Represents a generic interface for serialization and deserialization
 * operations of objects that extend the {@link WithChecksum} interface.
 * This interface provides functionality for loading and saving objects
 * from/to files or input streams, as well as verifying checksum integrity.
 *
 * @param <T> the type of the object handled by the serializer, must extend {@code Checksum}
 */
public interface ObjectSerializer<T extends WithChecksum> extends Closeable {

  /**
   * Loads an object of type T from the specified file.
   *
   * @param path the file from which the object will be loaded
   * @return the object of type T that has been deserialized from the file
   * @throws IOException if an I/O error occurs during reading from the file
   */
  T load(File path) throws IOException;

  /**
   * Loads an object of type T from the specified input stream.
   *
   * @param inputStream the input stream from which the object will be deserialized
   * @return the deserialized object of type T
   * @throws IOException if an I/O error occurs during reading from the input stream
   */
  T load(InputStream inputStream) throws IOException;

  /**
   * Serializes the given data object of type T and saves it to the specified file.
   *
   * @param path the file where the serialized object will be saved
   * @param data the object of type T to be serialized and saved
   * @throws IOException if an I/O error occurs during writing to the file
   */
  void save(File path, T data) throws IOException;

  /**
   * Verifies the checksum of the provided data object of type T.
   *
   * @param data the object of type T whose checksum is to be verified
   * @return true if the checksum of the data is valid, false otherwise
   * @throws IOException if an I/O error occurs during verification
   */
  boolean verifyChecksum(T data) throws IOException;

  @Override
  void close() throws IOException;
}
