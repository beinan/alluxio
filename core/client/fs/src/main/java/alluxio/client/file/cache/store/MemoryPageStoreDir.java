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

import static java.util.Objects.requireNonNull;

import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;

import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * Represents the dir and file level metadata of the MemPageStore.
 */
public class MemoryPageStoreDir extends QuotaManagedPageStoreDir {

  private final long mCapacity;
  private final Path mRootPath;
  private final MemoryPageStore mPageStore;

  /**
   * Constructor of MemPageStoreDir.
   * @param pageStoreOptions page store options
   * @param pageStore the PageStore instance
   * @param cacheEvictor the evictor
   */
  public MemoryPageStoreDir(PageStoreOptions pageStoreOptions,
                            MemoryPageStore pageStore,
                            CacheEvictor cacheEvictor) {
    super(pageStoreOptions.getRootDir(), pageStoreOptions.getCacheSize(), cacheEvictor);
    mPageStore = requireNonNull(pageStore);
    mCapacity = requireNonNull(pageStoreOptions.getCacheSize());
    mRootPath = requireNonNull(pageStoreOptions.getRootDir());
  }

  @Override
  public Path getRootPath() {
    return mRootPath;
  }

  @Override
  public PageStore getPageStore() {
    return mPageStore;
  }

  @Override
  public void resetPageStore() {
  }

  @Override
  public void restorePages(Consumer<PageInfo> pageInfoConsumer) {
    //do nothing
  }
}
