package alluxio.client.file.cache.store;

import static com.google.common.base.Preconditions.checkState;

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;

import com.google.common.collect.Streams;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Represent the dir and file level metadata of a rocksdb page store.
 */
public class RocksPageStoreDir implements PageStoreDir {

  private final long mCapacity;
  private final Path mRootPath;
  private final PageStoreOptions mPageStoreOptions;

  private RocksPageStore mPageStore;

  /**
   * Constructor of RocksPageStoreDir.
   * @param pageStoreOptions
   * @param pageStore
   */
  public RocksPageStoreDir(PageStoreOptions pageStoreOptions,
                           PageStore pageStore) {
    checkState(pageStore instanceof RocksPageStore);
    mPageStore = (RocksPageStore) pageStore;
    mPageStoreOptions = pageStoreOptions;
    mCapacity = pageStoreOptions.getCacheSize();
    mRootPath = pageStoreOptions.getRootDir();
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
  public long getCapacity() {
    return mCapacity;
  }

  @Override
  public void resetPageStore() {
    try {
      mPageStore.close();
      // when cache is large, e.g. millions of pages, initialize may take a while on deletion
      mPageStore = (RocksPageStore) PageStore.create(mPageStoreOptions);
    } catch (Exception e) {
      throw new RuntimeException("Reset page store failed for dir " + mRootPath.toString(), e);
    }
  }

  @Override
  public void restorePages(Consumer<PageInfo> pageInfoConsumer) throws IOException {
    RocksIterator iter = mPageStore.createNewInterator();
    iter.seekToFirst();
    Streams.stream(new PageIterator(iter, this)).onClose(iter::close).forEach(pageInfoConsumer);
  }

  @Override
  public long getCachedBytes() {
    return 0;
  }

  private class PageIterator implements Iterator<PageInfo> {
    private final RocksIterator mIter;
    private final RocksPageStoreDir mRocksPageStoreDir;
    private PageInfo mValue;

    PageIterator(RocksIterator iter,
                 RocksPageStoreDir rocksPageStoreDir) {
      mIter = iter;
      mRocksPageStoreDir = rocksPageStoreDir;
    }

    @Override
    public boolean hasNext() {
      return ensureValue() != null;
    }

    @Override
    public PageInfo next() {
      PageInfo value = ensureValue();
      if (value == null) {
        throw new NoSuchElementException();
      }
      mIter.next();
      mValue = null;
      return value;
    }

    @Nullable
    private PageInfo ensureValue() {
      if (mValue == null) {
        for (; mIter.isValid(); mIter.next()) {
          PageId id = RocksPageStore.getPageIdFromKey(mIter.key());
          long size = mIter.value().length;
          if (id != null) {
            mValue = new PageInfo(id, size, mRocksPageStoreDir);
            break;
          }
        }
      }
      return mValue;
    }
  }
}
