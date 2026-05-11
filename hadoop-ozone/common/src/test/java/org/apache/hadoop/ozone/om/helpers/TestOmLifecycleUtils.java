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

package org.apache.hadoop.ozone.om.helpers;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.Test;

/**
 * Test class for OmLifecycleUtils.
 */
public class TestOmLifecycleUtils {

  @Test
  public void testValidateTrashPrefix() {
    assertDoesNotThrow(() -> OmLifecycleUtils.validateTrashPrefix(null));
    assertDoesNotThrow(() -> OmLifecycleUtils.validateTrashPrefix(""));
    assertDoesNotThrow(() -> OmLifecycleUtils.validateTrashPrefix("normal/prefix"));
    assertDoesNotThrow(() -> OmLifecycleUtils.validateTrashPrefix("/normal/prefix"));
    assertDoesNotThrow(() -> OmLifecycleUtils.validateTrashPrefix(".TrashNot"));

    OMException ex1 = assertThrows(OMException.class,
        () -> OmLifecycleUtils.validateTrashPrefix(".Trash"));
    assertEquals(OMException.ResultCodes.INVALID_REQUEST, ex1.getResult());

    OMException ex2 = assertThrows(OMException.class,
        () -> OmLifecycleUtils.validateTrashPrefix("/.Trash"));
    assertEquals(OMException.ResultCodes.INVALID_REQUEST, ex2.getResult());

    OMException ex3 = assertThrows(OMException.class,
        () -> OmLifecycleUtils.validateTrashPrefix(".Trash/subpath"));
    assertEquals(OMException.ResultCodes.INVALID_REQUEST, ex3.getResult());
  }

  @Test
  public void testValidateAndNormalizePrefix() {
    assertDoesNotThrow(() -> OmLifecycleUtils.validateAndNormalizePrefix("normal/prefix"));
    
    OMException ex1 = assertThrows(OMException.class,
        () -> OmLifecycleUtils.validateAndNormalizePrefix("normal//prefix"));
    assertEquals(OMException.ResultCodes.INVALID_REQUEST, ex1.getResult());
  }

  @Test
  public void testValidatePrefixLength() {
    assertDoesNotThrow(() -> OmLifecycleUtils.validatePrefixLength(null));
    assertDoesNotThrow(() -> OmLifecycleUtils.validatePrefixLength("short/prefix"));
    
    String longPrefix = StringUtils.repeat("a", OmLifecycleUtils.MAX_PREFIX_LENGTH);
    assertDoesNotThrow(() -> OmLifecycleUtils.validatePrefixLength(longPrefix));

    String tooLongPrefix = StringUtils.repeat("a", OmLifecycleUtils.MAX_PREFIX_LENGTH + 1);
    OMException ex = assertThrows(OMException.class,
        () -> OmLifecycleUtils.validatePrefixLength(tooLongPrefix));
    assertEquals(OMException.ResultCodes.INVALID_REQUEST, ex.getResult());
  }

  @Test
  public void testValidateTagUniqAndLength() {
    assertDoesNotThrow(() -> OmLifecycleUtils.validateTagUniqAndLength(null));
    
    Map<String, String> validTags = new HashMap<>();
    validTags.put("key1", "value1");
    validTags.put("key2", "value2");
    assertDoesNotThrow(() -> OmLifecycleUtils.validateTagUniqAndLength(validTags));

    Map<String, String> emptyKeyTags = new HashMap<>();
    emptyKeyTags.put("", "value");
    OMException ex1 = assertThrows(OMException.class,
        () -> OmLifecycleUtils.validateTagUniqAndLength(emptyKeyTags));
    assertEquals(OMException.ResultCodes.INVALID_REQUEST, ex1.getResult());

    Map<String, String> tooLongKeyTags = new HashMap<>();
    tooLongKeyTags.put(StringUtils.repeat("k", OmLifecycleUtils.MAX_TAG_KEY_LENGTH + 1), "value");
    OMException ex2 = assertThrows(OMException.class,
        () -> OmLifecycleUtils.validateTagUniqAndLength(tooLongKeyTags));
    assertEquals(OMException.ResultCodes.INVALID_REQUEST, ex2.getResult());

    Map<String, String> tooLongValueTags = new HashMap<>();
    tooLongValueTags.put("key", StringUtils.repeat("v", OmLifecycleUtils.MAX_TAG_VALUE_LENGTH + 1));
    OMException ex3 = assertThrows(OMException.class,
        () -> OmLifecycleUtils.validateTagUniqAndLength(tooLongValueTags));
    assertEquals(OMException.ResultCodes.INVALID_REQUEST, ex3.getResult());
  }
}
