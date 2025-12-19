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

package org.apache.hadoop.ozone.s3.awssdk;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.ozone.test.InputSubstream;

/**
 * Utilities for S3 SDK tests.
 */
public final class S3SDKTestUtils {

  public static final Pattern UPLOAD_ID_PATTERN = Pattern.compile("<UploadId>(.+?)</UploadId>");

  private S3SDKTestUtils() {
  }

  /**
   * Calculate the MD5 digest from an input stream from a specific offset and length.
   * @param inputStream The input stream where the digest will be read from.
   *                    Note that the input stream will not be closed, the caller is responsible in closing
   *                    the input stream.
   * @param skip The byte offset to start the digest from.
   * @param length The number of bytes from the starting offset that will be digested.
   * @return byte array of the MD5 digest of the input stream from a specific offset and length.
   * @throws Exception exception.
   */
  public static byte[] calculateDigest(final InputStream inputStream, int skip, int length) throws Exception {
    int numRead;
    byte[] buffer = new byte[1024];

    MessageDigest complete = MessageDigest.getInstance("MD5");
    InputStream subStream = inputStream;
    if (skip > -1 && length > -1) {
      subStream = new InputSubstream(inputStream, skip, length);
    }

    do {
      numRead = subStream.read(buffer);
      if (numRead > 0) {
        complete.update(buffer, 0, numRead);
      }
    } while (numRead != -1);

    return complete.digest();
  }

  public static void createFile(File newFile, int size) throws IOException {
    // write random data so that filesystems with compression enabled (e.g. ZFS)
    // can't compress the file
    byte[] data = new byte[size];
    data = RandomUtils.secure().randomBytes(data.length);

    RandomAccessFile file = new RandomAccessFile(newFile, "rws");

    file.write(data);

    file.getFD().sync();
    file.close();
  }

  /**
   * Extract the UploadId from XML string.
   *
   * @param xml The XML string.
   * @return The UploadId, or null if not found.
   */
  public static String extractUploadId(String xml) {
    Matcher matcher = UPLOAD_ID_PATTERN.matcher(xml);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

  /**
   * Open an HttpURLConnection with the given parameters.
   *
   * @param url        The URL to connect to.
   * @param httpMethod The HTTP method to use (e.g., "GET", "PUT", "POST", etc.).
   * @param headers    A map of request headers to set. Can be null.
   * @param body       The request body as a byte array. Can be null.
   * @return An open HttpURLConnection.
   * @throws IOException If an I/O error occurs.
   */
  public static HttpURLConnection openHttpURLConnection(URL url, String httpMethod, Map<String, List<String>> headers,
                                                        byte[] body) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod(httpMethod);
    if (headers != null) {
      headers.forEach((key, values) -> values.forEach(value -> connection.addRequestProperty(key, value)));
    }

    if (body != null) {
      connection.setDoOutput(true);
      try (OutputStream os = connection.getOutputStream()) {
        IOUtils.write(body, os);
        os.flush();
      }
    }
    return connection;
  }
}
