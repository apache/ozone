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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3G_STS_TEST_MIN_DURATION_SECONDS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for S3STSUtils.
 */
public class TestS3STSUtils {

  private static final String DURATION_VALIDATION_ERROR_MESSAGE =
      "Invalid Value: DurationSeconds must be a number between 900 and 43200 seconds";

  @Test
  public void testValidateDurationStringNullUsesDefault() throws OMException {
    assertEquals(S3STSUtils.DEFAULT_DURATION_SECONDS, S3STSUtils.validateDuration((String) null));
  }

  @Test
  public void testValidateDurationStringBlankIsInvalid() {
    final OMException empty = assertThrows(OMException.class, () -> S3STSUtils.validateDuration(""));
    assertThat(empty.getMessage()).isEqualTo(DURATION_VALIDATION_ERROR_MESSAGE);

    final OMException whitespace = assertThrows(OMException.class, () -> S3STSUtils.validateDuration("   "));
    assertThat(whitespace.getMessage()).isEqualTo(DURATION_VALIDATION_ERROR_MESSAGE);
  }

  @Test
  public void testValidateDurationStringValidValues() throws OMException {
    assertEquals(900, S3STSUtils.validateDuration("900"));
    assertEquals(3600, S3STSUtils.validateDuration("3600"));
    assertEquals(43200, S3STSUtils.validateDuration("43200"));
  }

  @Test
  public void testValidateDurationStringOutOfRange() {
    final OMException tooShort = assertThrows(OMException.class, () -> S3STSUtils.validateDuration("899"));
    assertThat(tooShort.getMessage()).isEqualTo(DURATION_VALIDATION_ERROR_MESSAGE);

    final OMException tooLong = assertThrows(OMException.class, () -> S3STSUtils.validateDuration("43201"));
    assertThat(tooLong.getMessage()).isEqualTo(DURATION_VALIDATION_ERROR_MESSAGE);
  }

  @Test
  public void testValidateDurationStringOverflowsInt() {
    final OMException ex = assertThrows(OMException.class, () -> S3STSUtils.validateDuration("4320010000"));
    assertThat(ex.getMessage()).isEqualTo(DURATION_VALIDATION_ERROR_MESSAGE);
  }

  @Test
  public void testValidateDurationStringNonNumeric() {
    final OMException abc = assertThrows(OMException.class, () -> S3STSUtils.validateDuration("abc"));
    assertThat(abc.getMessage()).isEqualTo(DURATION_VALIDATION_ERROR_MESSAGE);

    final OMException decimal = assertThrows(OMException.class, () -> S3STSUtils.validateDuration("3.5"));
    assertThat(decimal.getMessage()).isEqualTo(DURATION_VALIDATION_ERROR_MESSAGE);

    final OMException invalidSign = assertThrows(OMException.class, () -> S3STSUtils.validateDuration("+-3"));
    assertThat(invalidSign.getMessage()).isEqualTo(DURATION_VALIDATION_ERROR_MESSAGE);
  }

  @Test
  public void testValidateDurationIntValidValues() throws OMException {
    assertEquals(900, S3STSUtils.validateDuration(900));
    assertEquals(3600, S3STSUtils.validateDuration(3600));
    assertEquals(43200, S3STSUtils.validateDuration(43200));
  }

  @Test
  public void testValidateDurationIntOutOfRange() {
    final OMException tooShort = assertThrows(OMException.class, () -> S3STSUtils.validateDuration(899));
    assertThat(tooShort.getMessage()).isEqualTo(DURATION_VALIDATION_ERROR_MESSAGE);

    final OMException tooLong = assertThrows(OMException.class, () -> S3STSUtils.validateDuration(43201));
    assertThat(tooLong.getMessage()).isEqualTo(DURATION_VALIDATION_ERROR_MESSAGE);
  }

  @Test
  public void testGetMinDurationSecondsDefault() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    assertEquals(900, S3STSUtils.getMinDurationSeconds(conf));
  }

  @Test
  public void testGetMinDurationSecondsOverride() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_S3G_STS_TEST_MIN_DURATION_SECONDS_KEY, 5);
    assertEquals(5, S3STSUtils.getMinDurationSeconds(conf));

    conf.setInt(OZONE_S3G_STS_TEST_MIN_DURATION_SECONDS_KEY, 1);
    assertEquals(1, S3STSUtils.getMinDurationSeconds(conf));

    conf.setInt(OZONE_S3G_STS_TEST_MIN_DURATION_SECONDS_KEY, S3STSUtils.MIN_DURATION_SECONDS);
    assertEquals(900, S3STSUtils.getMinDurationSeconds(conf));
  }

  @Test
  public void testGetMinDurationSecondsRejectsOutOfRange() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_S3G_STS_TEST_MIN_DURATION_SECONDS_KEY, 0);
    final IllegalArgumentException tooSmall = assertThrows(
        IllegalArgumentException.class, () -> S3STSUtils.getMinDurationSeconds(conf));
    assertEquals(
        "ozone.s3g.sts.test.min.duration.seconds must be between 1 and 900, but was 0",
        tooSmall.getMessage());

    conf.setInt(OZONE_S3G_STS_TEST_MIN_DURATION_SECONDS_KEY, 901);
    final IllegalArgumentException tooLarge = assertThrows(
        IllegalArgumentException.class, () -> S3STSUtils.getMinDurationSeconds(conf));
    assertEquals(
        "ozone.s3g.sts.test.min.duration.seconds must be between 1 and 900, but was 901",
        tooLarge.getMessage());
  }

  @Test
  public void testValidateDurationWithCustomMinimum() throws OMException {
    assertEquals(5, S3STSUtils.validateDuration(5, 5));

    final OMException tooShort = assertThrows(OMException.class, () -> S3STSUtils.validateDuration(4, 5));
    assertEquals(
        "Invalid Value: DurationSeconds must be a number between 5 and 43200 seconds", tooShort.getMessage());
  }

  @Test
  public void testValidateDurationStringWithCustomMinimum() throws OMException {
    assertEquals(5, S3STSUtils.validateDuration("5", 5));

    final OMException tooShort = assertThrows(OMException.class, () -> S3STSUtils.validateDuration("4", 5));
    assertEquals(
        "Invalid Value: DurationSeconds must be a number between 5 and 43200 seconds", tooShort.getMessage());

    final OMException nonNumeric = assertThrows(OMException.class, () -> S3STSUtils.validateDuration("abc", 5));
    assertEquals(
        "Invalid Value: DurationSeconds must be a number between 5 and 43200 seconds", nonNumeric.getMessage());
  }
}
