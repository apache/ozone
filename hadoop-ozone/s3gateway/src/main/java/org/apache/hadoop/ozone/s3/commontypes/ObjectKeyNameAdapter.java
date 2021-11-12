/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.commontypes;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


/**
 * A converter to convert raw-String to S3 compliant object key name.
 * ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
 */
public class ObjectKeyNameAdapter extends XmlAdapter<String, String> {

  private static final Charset UTF_8 = StandardCharsets.UTF_8;

  public ObjectKeyNameAdapter() {
  }

  @Override
  public String unmarshal(String s) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public String marshal(String s) throws Exception {
    return urlEncode(s);
  }

  private static String urlEncode(String str) {
    try {
      return URLEncoder.encode(str, UTF_8.name())
          .replaceAll("\\+", "%20");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

}
