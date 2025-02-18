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

package org.apache.hadoop.hdds.scm.storage;

import java.io.IOException;
import java.io.InputStream;

/**
 * A Reader interface to work with InputStream.
 */
public interface ByteReaderStrategy {
  /**
   * Read from a block using the InputStream.
   * @param is
   * @param numBytesToRead how many bytes to read
   * @return number of bytes read
   * @throws IOException
   */
  int readFromBlock(InputStream is, int numBytesToRead) throws IOException;

  /**
   * @return the target length to read.
   */
  int getTargetLength();
}
