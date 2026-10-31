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

import java.util.ArrayDeque;
import java.util.Deque;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 * SAX filter to force namespace usage.
 * <p>
 * This filter will read the XML content as namespace qualified content
 * independent from the current namespace usage.
 * <p>
 * It also reads value elements the way S3 does. JAXB keeps only the text after the last child element of an element
 * it reads as one value, so a Key holding <code>tmp/&lt;a/&gt;secret</code> would be read as {@code secret}. S3 reads
 * it as {@code tmp/secret}: inside a value, a child element holding nothing but XML whitespace is ignored and the text
 * around it is joined, and a child element holding any other text makes the body malformed. So does text directly
 * inside an element that holds child elements. Elements the request model does not define pass through unchanged,
 * and JAXB ignores them.
 */
public class XmlNamespaceFilter extends XMLFilterImpl {

  private String namespace;

  /** The model of each open element the request model defines, with the document at the bottom. */
  private final Deque<XmlElementModel> open = new ArrayDeque<>();

  /** Open elements the request model does not define. They and everything in them pass through unchecked. */
  private int unknownDepth;

  /**
   * Open elements inside a value. Their elements and text are not passed to JAXB. Processing instructions still are,
   * and JAXB ignores them; ignorable whitespace needs a DTD, which the parser refuses.
   */
  private int insideValueDepth;

  /**
   * Create the filter.
   *
   * @param namespace to add to every elements.
   * @param document how the request class reads the body, from {@link XmlElementModel#document(Class)}.
   */
  XmlNamespaceFilter(String namespace, XmlElementModel document) {
    this.namespace = namespace;
    open.push(document);
  }

  @Override
  public void startElement(String uri, String localName, String qName,
      Attributes atts) throws SAXException {
    if (insideValueDepth > 0 || unknownDepth == 0 && open.peek().isValue()) {
      insideValueDepth++;
      return;
    }
    if (unknownDepth > 0) {
      unknownDepth++;
    } else {
      XmlElementModel child = open.peek().child(elementName(localName, qName));
      if (child == null) {
        unknownDepth = 1;
      } else {
        open.push(child);
      }
    }
    super.startElement(namespace, localName, qName, atts);
  }

  @Override
  public void endElement(String uri, String localName, String qName)
      throws SAXException {
    if (insideValueDepth > 0) {
      insideValueDepth--;
      return;
    }
    if (unknownDepth > 0) {
      unknownDepth--;
    } else {
      open.pop();
    }
    super.endElement(namespace, localName, qName);
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    if (insideValueDepth > 0) {
      if (!isXmlWhitespace(ch, start, length)) {
        throw new SAXException("An element inside a value holds text");
      }
      return;
    }
    if (unknownDepth == 0 && !open.peek().isValue() && !isXmlWhitespace(ch, start, length)) {
      throw new SAXException("Text is not allowed in an element that holds other elements");
    }
    super.characters(ch, start, length);
  }

  /** The name JAXB matches: a parser that is not namespace-aware reports only the qualified name. */
  private static String elementName(String localName, String qName) {
    return localName == null || localName.isEmpty() ? qName : localName;
  }

  /** XML whitespace is space, tab, CR and LF only; S3 refuses any other character, such as U+3000, in these places. */
  private static boolean isXmlWhitespace(char[] ch, int start, int length) {
    for (int i = start; i < start + length; i++) {
      char c = ch[i];
      if (c != ' ' && c != '\t' && c != '\r' && c != '\n') {
        return false;
      }
    }
    return true;
  }
}
