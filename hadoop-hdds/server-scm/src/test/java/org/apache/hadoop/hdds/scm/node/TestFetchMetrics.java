package org.apache.hadoop.hdds.scm.node;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TestFetchMetrics {
  private static FetchMetrics fetchMetrics = new FetchMetrics();

  @BeforeEach
  void setUp() {
  }

  @AfterEach
  void tearDown() {
  }

  @Test
  public void testFetchAll() {
    String result = fetchMetrics.getMetrics(null);
    Pattern p = Pattern.compile("beans", Pattern.MULTILINE);
    Matcher m = p.matcher(result);
    assertTrue(m.find());
  }

  @Test
  public void testFetchFiltered() {
    String result = fetchMetrics.getMetrics("Hadoop:service=StorageContainerManager,name=NodeDecommissionMetrics");
    Pattern p = Pattern.compile("beans", Pattern.MULTILINE);
    Matcher m = p.matcher(result);
    assertTrue(m.find());
  }
}