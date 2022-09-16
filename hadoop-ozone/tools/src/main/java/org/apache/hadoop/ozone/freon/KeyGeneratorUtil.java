package org.apache.hadoop.ozone.freon;

import org.apache.commons.codec.digest.DigestUtils;

import java.util.function.Function;

public class KeyGeneratorUtil {
  public static final String pureIndex = "pureIndex";
  public static final String md5 = "md5";
  public static final String simpleHash = "simpleHash";
  public static final String fileDirSeparator = "/";

  char[] saltCharArray = {'m','D','E','T','t','q','c','j','X','8'};

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

  public String generateSimpleHashKeyName(int number) {
    if (number == 0) {
      return String.valueOf(saltCharArray[number]);
    }

    StringBuilder keyName = new StringBuilder();
    while (number > 0) {
      keyName.append(saltCharArray[number % 10]);
      number /= 10;
    }
    return keyName.toString();
  }

  public Function<Integer, String> simpleHashKeyNameFunc() {
    return number -> {
      if (number == 0) {
        return String.valueOf(saltCharArray[number]);
      }

      StringBuilder keyName = new StringBuilder();
      while (number > 0) {
        keyName.append(saltCharArray[number % 10]);
        number /= 10;
      }
      return keyName.toString();
    };
  }

}
