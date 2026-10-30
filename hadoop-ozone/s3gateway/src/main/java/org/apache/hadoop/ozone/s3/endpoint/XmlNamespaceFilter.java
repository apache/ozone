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
 * It also rejects mixed content: an element holding both text and child elements. No S3 request body has mixed
 * content, and JAXB keeps only the text after the last child, so a Key holding the text {@code a/}, an empty child
 * element and the text {@code b} would be read as {@code b} while other readers see {@code a/b}.
 */
public class XmlNamespaceFilter extends XMLFilterImpl {

  private String namespace;

  /** Per open element: whether it holds non-whitespace text, and whether it holds a child element. */
  private final Deque<boolean[]> content = new ArrayDeque<>();

  /**
   * Create the filter.
   *
   * @param namespace to add to every elements.
   */
  public XmlNamespaceFilter(String namespace) {
    this.namespace = namespace;
  }

  @Override
  public void startElement(String uri, String localName, String qName,
      Attributes atts) throws SAXException {
    boolean[] parent = content.peek();
    if (parent != null) {
      if (parent[0]) {
        throw new SAXException("Mixed content is not allowed: element " + qName + " follows text");
      }
      parent[1] = true;
    }
    content.push(new boolean[2]);
    super.startElement(namespace, localName, qName, atts);
  }

  @Override
  public void endElement(String uri, String localName, String qName)
      throws SAXException {
    content.pop();
    super.endElement(namespace, localName, qName);
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    boolean[] current = content.peek();
    if (current != null && !isWhitespace(ch, start, length)) {
      if (current[1]) {
        throw new SAXException("Mixed content is not allowed: text follows a child element");
      }
      current[0] = true;
    }
    super.characters(ch, start, length);
  }

  private static boolean isWhitespace(char[] ch, int start, int length) {
    for (int i = start; i < start + length; i++) {
      if (!Character.isWhitespace(ch[i])) {
        return false;
      }
    }
    return true;
  }
}
