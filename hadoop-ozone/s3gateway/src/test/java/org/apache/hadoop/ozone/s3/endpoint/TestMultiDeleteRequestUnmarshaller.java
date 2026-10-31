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

package org.apache.hadoop.ozone.s3.endpoint;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.stream.Stream;
import javax.ws.rs.WebApplicationException;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test custom marshalling of MultiDeleteRequest.
 * <p>
 * The parameterized cases are bodies sent to AWS S3 on 2026-10-31; the first argument is the case's name in that
 * test run, and the expected key or refusal is what S3 did with it.
 */
public class TestMultiDeleteRequestUnmarshaller {

  @Test
  public void fromStreamWithNamespace() throws IOException {
    //GIVEN
    ByteArrayInputStream inputBody =
        new ByteArrayInputStream(
            ("<Delete xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\"><Object><Key>key1</Key></Object><Object><Key>key2"
                + "</Key></Object><Object><Key>key3"
                + "</Key></Object></Delete>")
                .getBytes(UTF_8));

    //WHEN
    MultiDeleteRequest multiDeleteRequest =
        unmarshall(inputBody);

    //THEN
    assertEquals(3, multiDeleteRequest.getObjects().size());
  }

  @Test
  public void fromStreamWithoutNamespace() throws IOException {
    //GIVEN
    ByteArrayInputStream inputBody =
        new ByteArrayInputStream(
            ("<Delete><Object><Key>key1</Key></Object><Object><Key>key2"
                + "</Key></Object><Object><Key>key3"
                + "</Key></Object></Delete>")
                .getBytes(UTF_8));

    //WHEN
    MultiDeleteRequest multiDeleteRequest =
        unmarshall(inputBody);

    //THEN
    assertEquals(3, multiDeleteRequest.getObjects().size());
  }

  @Test
  public void keyWithEscapedMarkupIsOneKey() throws IOException {
    MultiDeleteRequest request = unmarshall(body("<Delete><Object><Key>tmp/&lt;x&gt;</Key></Object></Delete>"));
    assertEquals("tmp/<x>", request.getObjects().get(0).getKey());
  }

  @Test
  public void indentedBodyIsAccepted() throws IOException {
    MultiDeleteRequest request = unmarshall(body("<Delete>\n  <Object>\n    <Key>tmp/a</Key>\n  </Object>\n</Delete>"));
    assertEquals("tmp/a", request.getObjects().get(0).getKey());
  }

  /** Tab, CR and LF between elements are indentation (W07). */
  @Test
  public void indentationWithTabCarriageReturnAndLineFeedIsAccepted() throws IOException {
    MultiDeleteRequest request =
        unmarshall(body("<Delete>&#9;<Object>&#13;&#10;<Key>tmp/a</Key>&#10;</Object></Delete>"));
    assertEquals("tmp/a", request.getObjects().get(0).getKey());
  }

  /**
   * Inside a key, an element that holds no text, or only XML whitespace, is ignored and the text on both sides is
   * read as one key. Comments, CDATA sections and processing instructions are joined the same way.
   */
  @ParameterizedTest(name = "{0}: {1}")
  @MethodSource
  public void keyIsReadAsS3ReadsIt(String s3Case, String keyXml, String expectedKey) throws IOException {
    MultiDeleteRequest request = unmarshall(body("<Delete><Object><Key>" + keyXml + "</Key></Object></Delete>"));
    assertEquals(expectedKey, request.getObjects().get(0).getKey());
  }

  static Stream<Arguments> keyIsReadAsS3ReadsIt() {
    return Stream.of(
        arguments("D02", "tmp/<a/>secret", "tmp/secret"),
        arguments("D04", "tmp/<a/>", "tmp/"),
        arguments("D07", "<a/>tmp/x", "tmp/x"),
        arguments("D08", "tmp/x<a></a>y", "tmp/xy"),
        arguments("D09", "tmp/x<a> </a>y", "tmp/xy"),
        arguments("D10", "tmp/x<a b=\"1\"/>y", "tmp/xy"),
        arguments("D11", "tmp/x<a><b/></a>y", "tmp/xy"),
        arguments("D12", "tmp/x<!-- c -->y", "tmp/xy"),
        arguments("D13", "tmp/x<![CDATA[y]]>", "tmp/xy"),
        arguments("D14", "tmp/x<?pi z?>y", "tmp/xy"),
        arguments("D16", " <a/> ", "  "),
        arguments("W02", "tmp/x<a>&#9;&#10;</a>y", "tmp/xy"));
  }

  /** Inside a key, an element holding any other text is refused, at any depth (S3 answered 400 MalformedXML). */
  @ParameterizedTest(name = "{0}: {1}")
  @MethodSource
  public void keyHoldingAnElementWithTextIsRefused(String s3Case, String keyXml) {
    assertMalformed("<Delete><Object><Key>" + keyXml + "</Key></Object></Delete>");
  }

  static Stream<Arguments> keyHoldingAnElementWithTextIsRefused() {
    return Stream.of(
        arguments("D03", "<a>tmp/</a>secret"),
        arguments("D05", "<a>dir/</a> "),
        arguments("D06", "<a>dir/</a>"),
        arguments("D15", "<a>dir/</a>&#x3000;"),
        arguments("W01", "tmp/x<a>&#x3000;</a>y"),
        arguments("W03", "tmp/x<a>&#xA0;</a>y"),
        arguments("W04", "tmp/x<a>&#x2028;</a>y"),
        arguments("W05", "tmp/x<a><b>t</b></a>y"));
  }

  /** Text directly inside Object is refused (S3 answered 400 MalformedXML); only XML whitespace is indentation. */
  @ParameterizedTest(name = "{0}: {1}")
  @MethodSource
  public void textInsideObjectIsRefused(String s3Case, String objectXml) {
    assertMalformed("<Delete>" + objectXml + "</Delete>");
  }

  static Stream<Arguments> textInsideObjectIsRefused() {
    return Stream.of(
        arguments("D28", "<Object>junk<Key>tmp/a</Key></Object>"),
        arguments("W06", "<Object>&#x3000;<Key>tmp/a</Key></Object>"));
  }

  /**
   * An element the request model does not define is ignored with everything in it. S3 refuses it (D27), but the
   * gateway cannot tell S3 fields it does not model from junk, and ignoring them never changes the key.
   */
  @Test
  public void elementNotInTheModelIsIgnored() throws IOException {
    MultiDeleteRequest request =
        unmarshall(body("<Delete><Object><Key>tmp/a</Key><Foo>x<b/>y</Foo></Object></Delete>"));
    assertEquals("tmp/a", request.getObjects().get(0).getKey());
  }

  /** Checking resumes after an element the model does not define closes. */
  @Test
  public void keyAfterAnElementNotInTheModelIsReadAsS3ReadsIt() throws IOException {
    MultiDeleteRequest request =
        unmarshall(body("<Delete><Object><Foo/><Key>tmp/<a/>secret</Key></Object></Delete>"));
    assertEquals("tmp/secret", request.getObjects().get(0).getKey());
  }

  @Test
  public void textAfterAnElementNotInTheModelIsRefused() {
    assertMalformed("<Delete><Object><Foo/>junk<Key>tmp/a</Key></Object></Delete>");
  }

  @Test
  public void bodyThatIsNotWellFormedIsMalformedXml() {
    assertMalformed("<Delete><Object><Key>tmp/a</Key></Object>");
  }

  /** The body must be refused with S3's 400 MalformedXML, not read as another key. */
  private void assertMalformed(String xml) {
    WebApplicationException ex = assertThrows(WebApplicationException.class, () -> unmarshall(body(xml)));
    OS3Exception cause = assertInstanceOf(OS3Exception.class, ex.getCause());
    assertEquals(S3ErrorTable.MALFORMED_XML.getCode(), cause.getCode());
  }

  private static ByteArrayInputStream body(String xml) {
    return new ByteArrayInputStream(xml.getBytes(UTF_8));
  }

  private MultiDeleteRequest unmarshall(ByteArrayInputStream inputBody)
      throws IOException {
    return new MultiDeleteRequestUnmarshaller()
        .readFrom(null, null, null, null, null, inputBody);
  }
}
