/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.utils.db.managed;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;

/**
 * Managed RocksIterator.
 */
public class ManagedRocksIterator extends RocksIterator {
  private final RocksIterator original;

  public ManagedRocksIterator(RocksDB parent, RocksIterator original) {
    // This object just act as a proxy to the real iterator.
    super(parent, original.getNativeHandle());
    disOwnNativeHandle();

    this.original = original;
  }

  public static ManagedRocksIterator from(RocksDB parent,
                                          RocksIterator rocksIterator) {
    return new ManagedRocksIterator(parent, rocksIterator);
  }

  @Override
  public boolean isValid() {
    return original.isValid();
  }

  @Override
  public void seekToFirst() {
    original.seekToFirst();
  }

  @Override
  public void seekToLast() {
    original.seekToLast();
  }

  @Override
  public void seek(byte[] target) {
    original.seek(target);
  }

  @Override
  public void seekForPrev(byte[] target) {
    original.seekForPrev(target);
  }

  @Override
  public void seek(ByteBuffer target) {
    original.seek(target);
  }

  @Override
  public void seekForPrev(ByteBuffer target) {
    original.seekForPrev(target);
  }

  @Override
  public void next() {
    original.next();
  }

  @Override
  public void prev() {
    original.prev();
  }

  @Override
  public void status() throws RocksDBException {
    original.status();
  }

  @Override
  public void refresh() throws RocksDBException {
    original.refresh();
  }

  @Override
  public byte[] key() {
    return original.key();
  }

  @Override
  public int key(ByteBuffer key) {
    return original.key(key);
  }

  @Override
  public byte[] value() {
    return original.value();
  }

  @Override
  public int value(ByteBuffer value) {
    return original.value(value);
  }

  @Override
  public void close() {
    original.close();
  }

  @Override
  protected void finalize() throws Throwable {
    ManagedRocksObjectUtils.assertClosed(original);
    super.finalize();
  }
}
