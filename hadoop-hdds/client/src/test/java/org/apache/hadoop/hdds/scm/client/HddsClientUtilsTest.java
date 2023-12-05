package org.apache.hadoop.hdds.scm.client;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class HddsClientUtilsTest {

  @Test
  void testContainsException() {
    Exception ex1 = new ConnectException();
    Exception ex2 = new IOException(ex1);
    Exception ex3 = new IllegalArgumentException(ex2);

    assertSame(ex1,
        HddsClientUtils.containsException(ex3, ConnectException.class));
    assertSame(ex2,
        HddsClientUtils.containsException(ex3, IOException.class));
    assertSame(ex3,
        HddsClientUtils.containsException(ex3, IllegalArgumentException.class));
    assertNull(
        HddsClientUtils.containsException(ex3, IllegalStateException.class));
  }
}
