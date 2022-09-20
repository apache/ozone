/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.hadoop.ozone.freon;

import org.apache.commons.codec.digest.DigestUtils;

import java.util.function.Function;

/**
 * Utility class to generate key name from a given key index.
 */
public class KeyGeneratorUtil {
  public static final String PURE_INDEX = "pureIndex";
  public static final String MD5 = "md5";
//  public static final String SIMPLE_HASH = "simpleHash";
  public static final String FILE_DIR_SEPARATOR = "/";
//  private char[] saltCharArray =
//      {'m', 'D', 'E', 'T', 't', 'q', 'c', 'j', 'X', '8'};

  public String generatePureIndexKeyName(int number) {
    return String.valueOf(number);
  }
  public Function<Integer, String> pureIndexKeyNameFunc() {
    return number -> String.valueOf(number);
  }

  public String generateMd5KeyName(int number) {
    String encodedStr = DigestUtils.md5Hex(String.valueOf(number));
    return encodedStr.substring(0, 7);
  }

  public Function<Integer, String> md5KeyNameFunc() {
    return number -> DigestUtils.md5Hex(String.valueOf(number)).substring(0, 7);
  }

//  public String generateSimpleHashKeyName(int number) {
//    if (number == 0) {
//      return String.valueOf(saltCharArray[number]);
//    }
//
//    StringBuilder keyName = new StringBuilder();
//    while (number > 0) {
//      keyName.append(saltCharArray[number % 10]);
//      number /= 10;
//    }
//    return keyName.toString();
//  }

//  public Function<Integer, String> simpleHashKeyNameFunc() {
//    return number -> {
//      if (number == 0) {
//        return String.valueOf(saltCharArray[number]);
//      }
//
//      StringBuilder keyName = new StringBuilder();
//      while (number > 0) {
//        keyName.append(saltCharArray[number % 10]);
//        number /= 10;
//      }
//      return keyName.toString();
//    };
//  }
//
//  public char[] getSaltCharArray() {
//    return saltCharArray;
//  }
//  public void setSaltCharArray(char[] saltCharArray) {
//    this.saltCharArray = saltCharArray;
//  }
}
