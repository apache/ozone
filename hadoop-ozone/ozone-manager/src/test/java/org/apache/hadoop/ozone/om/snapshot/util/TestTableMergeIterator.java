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

package org.apache.hadoop.ozone.om.snapshot.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.StringInMemoryTestTable;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for TableMergeIterator.
 */
public class TestTableMergeIterator {

  /**
   * Tests basic constructor and initialization with multiple tables.
   */
  @Test
  public void testConstructorWithMultipleTables() throws RocksDatabaseException, CodecException {
    Iterator<String> keysToFilter = Arrays.asList("key1", "key2").iterator();
    String prefix = "prefix/";

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");
    StringInMemoryTestTable<String> table2 = new StringInMemoryTestTable<>("table2");

    table1.put("prefix/key1", "value1");
    table2.put("prefix/key2", "value2");

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, prefix, table1, table2);

    assertNotNull(mergeIterator, "TableMergeIterator should be created");

    mergeIterator.close();
  }

  /**
   * Tests hasNext() delegates to keysToFilter iterator.
   */
  @Test
  public void testHasNextDelegatesToKeysToFilter() throws RocksDatabaseException, CodecException {
    Iterator<String> keysToFilter = Arrays.asList("key1", "key2", "key3").iterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");
    table1.put("key1", "value1");

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1);

    assertTrue(mergeIterator.hasNext(), "Should have next element");
    mergeIterator.next();
    assertTrue(mergeIterator.hasNext(), "Should have next element");
    mergeIterator.next();
    assertTrue(mergeIterator.hasNext(), "Should have next element");
    mergeIterator.next();
    assertFalse(mergeIterator.hasNext(), "Should not have next element");

    mergeIterator.close();
  }

  /**
   * Tests next() retrieves values from all tables for a single key.
   */
  @Test
  public void testNextWithSingleKeyInAllTables() throws RocksDatabaseException, CodecException {
    String key = "key1";
    Iterator<String> keysToFilter = Collections.singletonList(key).iterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");
    StringInMemoryTestTable<String> table2 = new StringInMemoryTestTable<>("table2");

    table1.put(key, "value1");
    table2.put(key, "value2");

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1, table2);

    assertTrue(mergeIterator.hasNext());
    KeyValue<String, List<String>> result = mergeIterator.next();

    assertNotNull(result);
    assertEquals(key, result.getKey());
    assertEquals(2, result.getValue().size());
    assertEquals("value1", result.getValue().get(0));
    assertEquals("value2", result.getValue().get(1));

    mergeIterator.close();
  }

  /**
   * Tests next() when key is present in some tables but not others.
   */
  @Test
  public void testNextWithKeyInSomeTablesOnly() throws RocksDatabaseException, CodecException {
    String key = "key1";
    Iterator<String> keysToFilter = Collections.singletonList(key).iterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");
    StringInMemoryTestTable<String> table2 = new StringInMemoryTestTable<>("table2");
    StringInMemoryTestTable<String> table3 = new StringInMemoryTestTable<>("table3");

    // Table1 has key1
    table1.put(key, "value1");

    // Table2 doesn't have key1 (has a different key)
    table2.put("key2", "value2");

    // Table3 has key1
    table3.put(key, "value3");

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1, table2, table3);

    KeyValue<String, List<String>> result = mergeIterator.next();

    assertNotNull(result);
    assertEquals(key, result.getKey());
    assertEquals(3, result.getValue().size());
    assertEquals("value1", result.getValue().get(0));
    assertNull(result.getValue().get(1), "Table2 doesn't have key1, should be null");
    assertEquals("value3", result.getValue().get(2));

    mergeIterator.close();
  }

  /**
   * Tests next() when key is not present in any table.
   */
  @Test
  public void testNextWithKeyNotInAnyTable() throws RocksDatabaseException, CodecException {
    String key = "key1";
    Iterator<String> keysToFilter = Collections.singletonList(key).iterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");
    StringInMemoryTestTable<String> table2 = new StringInMemoryTestTable<>("table2");

    // Both tables have different keys
    table1.put("key2", "value1");
    table2.put("key3", "value2");

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1, table2);

    KeyValue<String, List<String>> result = mergeIterator.next();

    assertNotNull(result);
    assertEquals(key, result.getKey());
    assertEquals(2, result.getValue().size());
    assertNull(result.getValue().get(0), "Table1 doesn't have key1");
    assertNull(result.getValue().get(1), "Table2 doesn't have key1");

    mergeIterator.close();
  }

  /**
   * Tests next() with multiple keys in sequence.
   */
  @Test
  public void testNextWithMultipleKeys() throws RocksDatabaseException, CodecException {
    Iterator<String> keysToFilter = Arrays.asList("key1", "key2", "key3").iterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");
    StringInMemoryTestTable<String> table2 = new StringInMemoryTestTable<>("table2");

    // Setup table1: key1->v1, key2->v2, key3->v3
    table1.put("key1", "v1");
    table1.put("key2", "v2");
    table1.put("key3", "v3");

    // Setup table2: key1->v1b, key3->v3b (no key2)
    table2.put("key1", "v1b");
    table2.put("key3", "v3b");
    table2.put("key4", "v4b");

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1, table2);

    // First key: key1
    KeyValue<String, List<String>> result1 = mergeIterator.next();
    assertEquals("key1", result1.getKey());
    assertEquals("v1", result1.getValue().get(0));
    assertEquals("v1b", result1.getValue().get(1));

    // Second key: key2
    KeyValue<String, List<String>> result2 = mergeIterator.next();
    assertEquals("key2", result2.getKey());
    assertEquals("v2", result2.getValue().get(0));
    assertNull(result2.getValue().get(1));

    // Third key: key3
    KeyValue<String, List<String>> result3 = mergeIterator.next();
    assertEquals("key3", result3.getKey());
    assertEquals("v3", result3.getValue().get(0));
    assertEquals("v3b", result3.getValue().get(1));

    mergeIterator.close();
  }

  /**
   * Tests next() with empty tables.
   */
  @Test
  public void testNextWithEmptyTables() throws RocksDatabaseException, CodecException {
    String key = "key1";
    Iterator<String> keysToFilter = Collections.singletonList(key).iterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");
    StringInMemoryTestTable<String> table2 = new StringInMemoryTestTable<>("table2");

    // Both tables are empty - no puts

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1, table2);

    KeyValue<String, List<String>> result = mergeIterator.next();

    assertNotNull(result);
    assertEquals(key, result.getKey());
    assertEquals(2, result.getValue().size());
    assertNull(result.getValue().get(0));
    assertNull(result.getValue().get(1));

    mergeIterator.close();
  }

  /**
   * Tests next() throws NoSuchElementException when hasNext() is false.
   */
  @Test
  public void testNextThrowsWhenNoMoreElements() throws RocksDatabaseException, CodecException {
    Iterator<String> keysToFilter = Collections.emptyIterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1);

    assertFalse(mergeIterator.hasNext());
    assertThrows(NoSuchElementException.class, mergeIterator::next);

    mergeIterator.close();
  }

  /**
   * Tests close() closes the merge iterator without errors.
   */
  @Test
  public void testClose() throws RocksDatabaseException, CodecException {
    Iterator<String> keysToFilter = Collections.emptyIterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");
    StringInMemoryTestTable<String> table2 = new StringInMemoryTestTable<>("table2");
    StringInMemoryTestTable<String> table3 = new StringInMemoryTestTable<>("table3");

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1, table2, table3);

    // Close should not throw exception
    mergeIterator.close();
  }

  /**
   * Tests with prefix filtering.
   */
  @Test
  public void testWithPrefix() throws RocksDatabaseException, CodecException {
    String prefix = "prefix/";
    String key = "prefix/key1";
    Iterator<String> keysToFilter = Collections.singletonList(key).iterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");
    StringInMemoryTestTable<String> table2 = new StringInMemoryTestTable<>("table2");

    table1.put(key, "value1");
    table1.put("other/key", "otherValue");
    table2.put(key, "value2");

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, prefix, table1, table2);

    KeyValue<String, List<String>> result = mergeIterator.next();

    assertEquals(key, result.getKey());
    assertEquals("value1", result.getValue().get(0));
    assertEquals("value2", result.getValue().get(1));

    mergeIterator.close();
  }

  /**
   * Tests with single table.
   */
  @Test
  public void testWithSingleTable() throws RocksDatabaseException, CodecException {
    String key = "key1";
    Iterator<String> keysToFilter = Collections.singletonList(key).iterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");
    table1.put(key, "value1");

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1);

    KeyValue<String, List<String>> result = mergeIterator.next();

    assertEquals(key, result.getKey());
    assertEquals(1, result.getValue().size());
    assertEquals("value1", result.getValue().get(0));

    mergeIterator.close();
  }

  /**
   * Tests that the values list is mutable and updated on each next() call.
   * This verifies the documented behavior that the returned list is not immutable
   * and will be modified on subsequent calls.
   */
  @Test
  public void testValuesListIsMutableAndReused() throws RocksDatabaseException, CodecException {
    Iterator<String> keysToFilter = Arrays.asList("key1", "key2").iterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");
    table1.put("key1", "value1");
    table1.put("key2", "value2");

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1);

    KeyValue<String, List<String>> result1 = mergeIterator.next();
    List<String> values1 = result1.getValue();
    assertEquals("value1", values1.get(0));

    KeyValue<String, List<String>> result2 = mergeIterator.next();
    List<String> values2 = result2.getValue();
    assertEquals("value2", values2.get(0));

    // The lists should be the same instance (reused)
    assertTrue(values1 == values2, "Values list should be reused across next() calls");

    mergeIterator.close();
  }

  /**
   * Tests with null key in keysToFilter.
   */
  @Test
  public void testWithNullKey() throws RocksDatabaseException, CodecException {
    String nullKey = null;
    Iterator<String> keysToFilter = Collections.singletonList(nullKey).iterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1);

    KeyValue<String, List<String>> result = mergeIterator.next();

    assertNull(result.getKey());
    assertEquals(1, result.getValue().size());
    assertNull(result.getValue().get(0));

    mergeIterator.close();
  }

  /**
   * Tests with large dataset to verify performance characteristics.
   */
  @Test
  public void testWithLargeDataset() throws RocksDatabaseException, CodecException {
    // Create 100 keys to filter
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      keys.add("key" + String.format("%03d", i));
    }
    Iterator<String> keysToFilter = keys.iterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");
    StringInMemoryTestTable<String> table2 = new StringInMemoryTestTable<>("table2");

    // Populate tables - table1 has even keys, table2 has odd keys
    for (int i = 0; i < 100; i++) {
      String key = "key" + String.format("%03d", i);
      if (i % 2 == 0) {
        table1.put(key, "value1-" + i);
      } else {
        table2.put(key, "value2-" + i);
      }
    }

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1, table2);

    int count = 0;
    while (mergeIterator.hasNext()) {
      KeyValue<String, List<String>> result = mergeIterator.next();
      int index = count;
      assertEquals("key" + String.format("%03d", index), result.getKey());
      
      if (index % 2 == 0) {
        // Even keys in table1
        assertEquals("value1-" + index, result.getValue().get(0));
        assertNull(result.getValue().get(1));
      } else {
        // Odd keys in table2
        assertNull(result.getValue().get(0));
        assertEquals("value2-" + index, result.getValue().get(1));
      }
      count++;
    }

    assertEquals(100, count, "Should have processed all 100 keys");

    mergeIterator.close();
  }

  /**
   * Tests with gaps in the key sequence.
   */
  @Test
  public void testWithKeyGaps() throws RocksDatabaseException, CodecException {
    // Filter requests specific keys with gaps
    Iterator<String> keysToFilter = Arrays.asList("key01", "key05", "key10").iterator();

    StringInMemoryTestTable<String> table1 = new StringInMemoryTestTable<>("table1");

    // Table has more keys than requested
    for (int i = 1; i <= 15; i++) {
      table1.put(String.format("key%02d", i), "value" + i);
    }

    TableMergeIterator<String, String> mergeIterator =
        new TableMergeIterator<>(keysToFilter, null, table1);

    // Should only get the requested keys
    KeyValue<String, List<String>> result1 = mergeIterator.next();
    assertEquals("key01", result1.getKey());
    assertEquals("value1", result1.getValue().get(0));

    KeyValue<String, List<String>> result2 = mergeIterator.next();
    assertEquals("key05", result2.getKey());
    assertEquals("value5", result2.getValue().get(0));

    KeyValue<String, List<String>> result3 = mergeIterator.next();
    assertEquals("key10", result3.getKey());
    assertEquals("value10", result3.getValue().get(0));

    assertFalse(mergeIterator.hasNext());

    mergeIterator.close();
  }
}

