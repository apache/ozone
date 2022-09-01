///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with this
// * work for additional information regarding copyright ownership.  The ASF
// * licenses this file to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except
// in compliance with the License.
// * You may obtain a copy of the License at
// * <p>
// * http://www.apache.org/licenses/LICENSE-2.0
// * <p>
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing
// permissions and limitations under
// * the License.
// */
//package org.apache.hadoop.ozone.freon;
//
//import org.apache.commons.codec.binary.Base64;
//import org.apache.commons.codec.digest.DigestUtils;
//import org.apache.hadoop.hdds.cli.HddsVersionProvider;
//import picocli.CommandLine;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//
//import java.io.*;
//import java.security.MessageDigest;
//import java.util.HashMap;
//import java.util.concurrent.Callable;
//
//@CommandLine.Command(name = "base64map",
//        description =
//                "Generate from 0 to 1000000 base64 encoding mapping file",
//        versionProvider = HddsVersionProvider.class,
//        mixinStandardHelpOptions = true,
//        showDefaultValues = true)
//public class GenerateBase64Mapping extends BaseFreonGenerator
//        implements Callable<Void> {
//
//  @CommandLine.Option(names = {"--clients"},
//          description =
//                  "Number of clients, defaults 1.",
//          defaultValue = "1")
//  private int clientsCount = 1;
//
//  public HashMap<Integer, String> intToBase64Encode = new HashMap();
//  @Override
//  public Void call() throws Exception {
////    File fout = new File("./int2Base64EncodeMap.txt");
////    FileOutputStream fos = new FileOutputStream(fout);
////
////    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
//
////    for (int i = 100000; i < 1100000; i++){
//    for (int i = 0; i < 100000; i++){
//
//      //encoding  byte array into base 64
////      byte[] encoded = Base64.encodeBase64(String.valueOf(i).getBytes());
////      byte[] encoded = String.valueOf(i).getBytes("UTF-8");
////
////      MessageDigest md = MessageDigest.getInstance("MD5");
////      byte[] theMD5digest = md.digest(encoded);
////      bw.write(i + "," + new String(encoded).substring(0,7));
////      bw.newLine();
////      String encodedStr = new String(theMD5digest);
//      String encodedStr = DigestUtils.md5Hex(String.valueOf(i));
//      intToBase64Encode.put(i, encodedStr.substring(0,7));
//    }
////    bw.close();
//    ObjectMapper mapper = new ObjectMapper();
//    mapper.writeValue(new File("./int2Base64EncodeMap.json"),
//    intToBase64Encode);
//
//
//    return null;
//  }
//
//}
