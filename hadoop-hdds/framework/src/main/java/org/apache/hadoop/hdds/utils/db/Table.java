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

package org.apache.hadoop.hdds.utils.db;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.TableCacheMetrics;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

/**
 * Interface for key-value store that stores ozone metadata. Ozone metadata is
 * stored as key value pairs, both key and value are arbitrary byte arrays. Each
 * Table Stores a certain kind of keys and values. This allows a DB to have
 * different kind of tables.
 */
@InterfaceStability.Evolving
public interface Table<KEY, VALUE> extends AutoCloseable {

  /**
   * Puts a key-value pair into the store.
   *
   * @param key metadata key
   * @param value metadata value
   */
  void put(KEY key, VALUE value) throws IOException;

  /**
   * Puts a key-value pair into the store as part of a bath operation.
   *
   * @param batch the batch operation
   * @param key metadata key
   * @param value metadata value
   */
  void putWithBatch(BatchOperation batch, KEY key, VALUE value)
      throws IOException;

  /**
   * @return true if the metadata store is empty.
   * @throws IOException on Failure
   */
  boolean isEmpty() throws IOException;

  /**
   * Check if a given key exists in Metadata store.
   * (Optimization to save on data deserialization)
   * A lock on the key / bucket needs to be acquired before invoking this API.
   * @param key metadata key
   * @return true if the metadata store contains a key.
   * @throws IOException on Failure
   */
  boolean isExist(KEY key) throws IOException;

  /**
   * Returns the value mapped to the given key in byte array or returns null
   * if the key is not found.
   *
   * @param key metadata key
   * @return value in byte array or null if the key is not found.
   * @throws IOException on Failure
   */
  VALUE get(KEY key) throws IOException;

  /**
   * Skip checking cache and get the value mapped to the given key in byte
   * array or returns null if the key is not found.
   *
   * @param key metadata key
   * @return value in byte array or null if the key is not found.
   * @throws IOException on Failure
   */
  default VALUE getSkipCache(KEY key) throws IOException {
    throw new NotImplementedException("getSkipCache is not implemented");
  }

  /**
   * Returns the value mapped to the given key in byte array or returns null
   * if the key is not found.
   *
   * This method is specific to tables implementation. Refer java doc of the
   * implementation for the behavior.
   *
   * @param key metadata key
   * @return value in byte array or null if the key is not found.
   * @throws IOException on Failure
   */
  default VALUE getReadCopy(KEY key) throws IOException {
    throw new NotImplementedException("getReadCopy is not implemented");
  }

  /**
   * Returns the value mapped to the given key in byte array or returns null
   * if the key is not found.
   *
   * This method first checks using keyMayExist, if it returns false, we are
   * 100% sure that key does not exist in DB, so it returns null with out
   * calling db.get. If keyMayExist return true, then we use db.get and then
   * return the value. This method will be useful in the cases where the
   * caller is more sure that this key does not exist in DB and keyMayExist
   * will help here.
   *
   * @param key metadata key
   * @return value in byte array or null if the key is not found.
   * @throws IOException on Failure
   */
  VALUE getIfExist(KEY key) throws IOException;

  /**
   * Deletes a key from the metadata store.
   *
   * @param key metadata key
   * @throws IOException on Failure
   */
  void delete(KEY key) throws IOException;

  /**
   * Deletes a key from the metadata store as part of a batch operation.
   *
   * @param batch the batch operation
   * @param key metadata key
   * @throws IOException on Failure
   */
  void deleteWithBatch(BatchOperation batch, KEY key) throws IOException;

  /**
   * Deletes a range of keys from the metadata store.
   *
   * @param beginKey start metadata key
   * @param endKey end metadata key
   * @throws IOException on Failure
   */
  void deleteRange(KEY beginKey, KEY endKey) throws IOException;

  /** The same as iterator(null). */
  default KeyValueIterator<KEY, VALUE> iterator() throws IOException {
    return iterator(null);
  }

  /** The same as iterator(prefix, KEY_AND_VALUE). */
  default KeyValueIterator<KEY, VALUE> iterator(KEY prefix) throws IOException {
    return iterator(prefix, KeyValueIterator.Type.KEY_AND_VALUE);
  }

  /**
   * Iterate the elements in this table.
   *
   * @param prefix The prefix of the elements to be iterated.
   * @param type Specify whether key and/or value are required.
   * @return an iterator.
   */
  KeyValueIterator<KEY, VALUE> iterator(KEY prefix, KeyValueIterator.Type type)
      throws IOException;

  /**
   * Returns the Name of this Table.
   * @return - Table Name.
   */
  String getName();

  /**
   * Returns the key count of this Table.  Note the result can be inaccurate.
   * @return Estimated key count of this Table
   * @throws IOException on failure
   */
  long getEstimatedKeyCount() throws IOException;

  /**
   * Add entry to the table cache.
   *
   * If the cacheKey already exists, it will override the entry.
   * @param cacheKey
   * @param cacheValue
   */
  default void addCacheEntry(CacheKey<KEY> cacheKey,
      CacheValue<VALUE> cacheValue) {
    throw new NotImplementedException("addCacheEntry is not implemented");
  }

  /** Add entry to the table cache with a non-null key and a null value. */
  default void addCacheEntry(KEY cacheKey, long epoch) {
    addCacheEntry(new CacheKey<>(cacheKey), CacheValue.get(epoch));
  }

  /** Add entry to the table cache with a non-null key and a non-null value. */
  default void addCacheEntry(KEY cacheKey, VALUE value, long epoch) {
    addCacheEntry(new CacheKey<>(cacheKey),
        CacheValue.get(epoch, value));
  }

  /**
   * Get the cache value from table cache.
   * @param cacheKey
   */
  default CacheValue<VALUE> getCacheValue(CacheKey<KEY> cacheKey) {
    throw new NotImplementedException("getCacheValue is not implemented");
  }

  /**
   * Removes all the entries from the table cache which are matching with
   * epoch provided in the epoch list.
   * @param epochs
   */
  default void cleanupCache(List<Long> epochs) {
    throw new NotImplementedException("cleanupCache is not implemented");
  }

  /**
   * Return cache iterator maintained for this table.
   */
  default Iterator<Map.Entry<CacheKey<KEY>, CacheValue<VALUE>>>
      cacheIterator() {
    throw new NotImplementedException("cacheIterator is not implemented");
  }

  /**
   * Create the metrics datasource that emits table cache metrics.
   */
  default TableCacheMetrics createCacheMetrics() throws IOException {
    throw new NotImplementedException("getCacheValue is not implemented");
  }

  /**
   * Returns a certain range of key value pairs as a list based on a
   * startKey or count. Further a {@link org.apache.hadoop.hdds.utils.MetadataKeyFilters.MetadataKeyFilter}
   * can be added to * filter keys if necessary.
   * To prevent race conditions while listing
   * entries, this implementation takes a snapshot and lists the entries from
   * the snapshot. This may, on the other hand, cause the range result slight
   * different with actual data if data is updating concurrently.
   * <p>
   * If the startKey is specified and found in the table, this key and the keys
   * after this key will be included in the result. If the startKey is null
   * all entries will be included as long as other conditions are satisfied.
   * If the given startKey doesn't exist and empty list will be returned.
   * <p>
   * The count argument is to limit number of total entries to return,
   * the value for count must be an integer greater than 0.
   * <p>
   * This method allows to specify one or more
   * {@link org.apache.hadoop.hdds.utils.MetadataKeyFilters.MetadataKeyFilter}
   * to filter keys by certain condition. Once given, only the entries
   * whose key passes all the filters will be included in the result.
   *
   * @param startKey a start key.
   * @param count max number of entries to return.
   * @param prefix fixed key schema specific prefix
   * @param filters customized one or more
   * {@link org.apache.hadoop.hdds.utils.MetadataKeyFilters.MetadataKeyFilter}.
   * @return a list of entries found in the database or an empty list if the
   * startKey is invalid.
   * @throws IOException if there are I/O errors.
   * @throws IllegalArgumentException if count is less than 0.
   */
  List<KeyValue<KEY, VALUE>> getRangeKVs(KEY startKey,
          int count, KEY prefix,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException;

  /**
   * This method is very similar to {@link #getRangeKVs}, the only
   * different is this method is supposed to return a sequential range
   * of elements based on the filters. While iterating the elements,
   * if it met any entry that cannot pass the filter, the iterator will stop
   * from this point without looking for next match. If no filter is given,
   * this method behaves just like {@link #getRangeKVs}.
   *
   * @param startKey a start key.
   * @param count max number of entries to return.
   * @param prefix fixed key schema specific prefix
   * @param filters customized one or more
   * {@link org.apache.hadoop.hdds.utils.MetadataKeyFilters.MetadataKeyFilter}.
   * @return a list of entries found in the database.
   * @throws IOException
   * @throws IllegalArgumentException
   */
  List<KeyValue<KEY, VALUE>> getSequentialRangeKVs(KEY startKey,
          int count, KEY prefix,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException;

  /**
   * Deletes all keys with the specified prefix from the metadata store
   * as part of a batch operation.
   * @param batch
   * @param prefix
   */
  void deleteBatchWithPrefix(BatchOperation batch, KEY prefix)
      throws IOException;

  /**
   * Dump all key value pairs with a prefix into an external file.
   * @param externalFile
   * @param prefix
   * @throws IOException
   */
  void dumpToFileWithPrefix(File externalFile, KEY prefix) throws IOException;

  /**
   * Load key value pairs from an external file created by
   * dumpToFileWithPrefix.
   * @param externalFile
   * @throws IOException
   */
  void loadFromFile(File externalFile) throws IOException;

  /**
   * Class used to represent the key and value pair of a db entry.
   */
  final class KeyValue<K, V> {
    private final K key;
    private final V value;
    private final int valueByteSize;

    private KeyValue(K key, V value, int valueByteSize) {
      this.key = key;
      this.value = value;
      this.valueByteSize = valueByteSize;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    public int getValueByteSize() {
      return valueByteSize;
    }

    @Override
    public String toString() {
      return "(key=" + key + ", value=" + value + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof KeyValue)) {
        return false;
      }
      final KeyValue<?, ?> that = (KeyValue<?, ?>) obj;
      return this.getKey().equals(that.getKey())
          && this.getValue().equals(that.getValue());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getKey(), getValue());
    }
  }

  static <K, V> KeyValue<K, V> newKeyValue(K key, V value) {
    return newKeyValue(key, value, 0);
  }

  static <K, V> KeyValue<K, V> newKeyValue(K key, V value, int valueByteSize) {
    return new KeyValue<>(key, value, valueByteSize);
  }

  /** A {@link TableIterator} to iterate {@link KeyValue}s. */
  interface KeyValueIterator<KEY, VALUE>
      extends TableIterator<KEY, KeyValue<KEY, VALUE>> {

    /** The iterator type. */
    enum Type {
      /** Neither read key nor value. */
      NEITHER,
      /** Read key only. */
      KEY_ONLY,
      /** Read value only. */
      VALUE_ONLY,
      /** Read both key and value. */
      KEY_AND_VALUE;

      boolean readKey() {
        return (this.ordinal() & KEY_ONLY.ordinal()) != 0;
      }

      boolean readValue() {
        return (this.ordinal() & VALUE_ONLY.ordinal()) != 0;
      }
    }
  }
}
