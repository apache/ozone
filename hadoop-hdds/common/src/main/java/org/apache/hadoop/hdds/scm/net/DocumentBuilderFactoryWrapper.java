package org.apache.hadoop.hdds.scm.net;

import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Wrapper for DocumentBuilderFactory reduces the initialization amount.
 */
public final class DocumentBuilderFactoryWrapper {
  private static DocumentBuilderFactory factory;

  private DocumentBuilderFactoryWrapper() {
  }

  public static DocumentBuilderFactory getInstance() {
    if (factory == null) {
      factory = DocumentBuilderFactory.newInstance();
    }
    return factory;
  }
}
