package org.apache.hadoop.hdds;

public interface StorageClassRegistry {

  StorageClass getStorageClass(String name);
}
