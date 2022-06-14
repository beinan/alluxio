/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache.store;

import static com.google.common.base.Preconditions.checkState;

import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.conf.AlluxioConfiguration;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

/**
 * Directory of page store.
 */
public interface PageStoreDir {
  /**
   * Create a list of PageStoreDir based on the configuration.
   * @param conf AlluxioConfiguration
   * @return A list of LocalCacheDir
   * @throws IOException
   */
  static List<PageStoreDir> createPageStoreDirs(AlluxioConfiguration conf)
      throws IOException {
    return PageStoreOptions.create(conf).stream().map(options -> createPageStoreDir(conf, options))
        .collect(ImmutableList.toImmutableList());
  }

  /**
   * Create an instance of PageStoreDir.
   *
   * @param conf
   * @param pageStoreOptions
   * @return PageStoreDir
   */
  static PageStoreDir createPageStoreDir(AlluxioConfiguration conf,
                                         PageStoreOptions pageStoreOptions) {
    switch (pageStoreOptions.getType()) {
      case LOCAL:
        checkState(pageStoreOptions instanceof LocalPageStoreOptions);
        return new LocalPageStoreDir(
            (LocalPageStoreOptions) pageStoreOptions,
            PageStore.openOrCreatePageStore(pageStoreOptions),
            CacheEvictor.create(conf)
        );
      case ROCKS:
        return new RocksPageStoreDir(
            pageStoreOptions,
            PageStore.openOrCreatePageStore(pageStoreOptions),
            CacheEvictor.create(conf)
        );
      case MEM:
        return new MemoryPageStoreDir(
            pageStoreOptions,
            (MemoryPageStore) PageStore.openOrCreatePageStore(pageStoreOptions),
            CacheEvictor.create(conf)
        );
      default:
        throw new IllegalArgumentException(String.format("Unrecognized store type %s",
            pageStoreOptions.getType().name()));
    }
  }

  /**
   * @param fileBuckets number of buckets
   * @param fileId file id
   * @return file bucket
   */
  static String getFileBucket(int fileBuckets, String fileId) {
    return Integer.toString(Math.floorMod(fileId.hashCode(), fileBuckets));
  }

  /**
   * @return root path
   */
  Path getRootPath();

  /**
   * @return pageStore
   */
  PageStore getPageStore();

  /**
   * @return capacity
   */
  long getCapacityBytes();

  /**
   * Reset page store.
   */
  void reset();

  /**
   * Scan the pages under this dir.
   * @param pageInfoConsumer
   * @throws IOException
   */
  void scanPages(Consumer<PageInfo> pageInfoConsumer) throws IOException;

  /**
   * @return cached bytes in this directory
   */
  long getCachedBytes();

  /**
   * @param pageInfo
   * @return if the page added successfully
   */
  boolean putPage(PageInfo pageInfo);

  /**
   * @param fileId
   * @return if the fileId added successfully
   */
  boolean putTempFile(String fileId);

  /**
   * @param bytes
   * @return if the bytes requested could be reserved
   */
  boolean reserve(int bytes);

  /**
   * @param bytes
   * @return the bytes used after release
   */
  long deletePage(PageInfo bytes);

  /**
   * Release the pre-reserved space.
   * @param bytes
   * @return the bytes used after the release
   */
  long release(int bytes);

  /**
   * @param fileId
   * @return true if the file is contained, false otherwise
   */
  boolean hasFile(String fileId);

  /**
   * @return the evictor of this dir
   */
  CacheEvictor getEvictor();

  /**
   * Close the page store dir.
   */
  void close();
}
