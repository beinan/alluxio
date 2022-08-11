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

package alluxio.worker;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.CreateBlockOptions;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

@Fork(value = 1, jvmArgsPrepend = "-server")
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
public class BlockStoreReadBench {
  private static final int MAX_SIZE = 64 * 1024 * 1024;

  /**
   * A mock consumer of the data read from the store.
   */
  private static final byte[] SINK = new byte[MAX_SIZE];

  @State(Scope.Benchmark)
  public static class BlockStoreParams {
    private final Random mRandom = new Random();
    @Param({"16", "64"})
    public long mBlockSizeMB;

    /** Derived from mBlockSizeMB. */
    protected long mBlockSizeByte;

    BlockStoreBase mBlockStoreBase;

    /** Local Block id that has been cached. */
    protected final long mLocalBlockId = 1L;

    /** ufs mount id. */
    protected final long mUfsMountId = 10L;

    /** ufs file path that is not cached yet. */
    protected String mUfsPath;

    /** ufs block id. */
    protected long mUfsBlockId = 3L;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      mBlockSizeByte = mBlockSizeMB * 1024L * 1024L;
      mBlockStoreBase = BlockStoreBase.create();

      // prepare some random data
      byte[] data = new byte[(int) mBlockSizeByte];
      mRandom.nextBytes(data);

      prepareLocalBlock(data);

      // ufs block is used by both Mono and Paged block store
      prepareUfsBlock(data);
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
      mBlockStoreBase.close();
    }

    private void prepareLocalBlock(byte[] data) throws Exception {
      // create local block
      mBlockStoreBase.mMonoBlockStore
              .createBlock(1, mLocalBlockId, 0,
                      new CreateBlockOptions(null, null, mBlockSizeByte));
      try (BlockWriter writer = mBlockStoreBase.mMonoBlockStore
          .createBlockWriter(1, mLocalBlockId)) {
        writer.append(ByteBuffer.wrap(data));
      }
      mBlockStoreBase.mMonoBlockStore.commitBlock(1, mLocalBlockId, false);

      // todo(yangchen): create local block for PagedBlockStore
    }

    private void prepareUfsBlock(byte[] data) throws Exception {
      // set up ufs root
      File ufsRoot = AlluxioTestDirectory.createTemporaryDirectory("ufs");
      mBlockStoreBase.mUfsManager.addMount(
              mUfsMountId, new AlluxioURI(ufsRoot.getAbsolutePath()),
              UnderFileSystemConfiguration.defaults(Configuration.global()));

      // create ufs block file
      mUfsPath = PathUtils.concatUfsPath(ufsRoot.getAbsolutePath(), "file1");
      File ufsFile = new File(mUfsPath);
      if (!ufsFile.createNewFile()) {
        throw new RuntimeException("Failed to create ufs file");
      }
      try (FileOutputStream out = new FileOutputStream(ufsFile);
           BufferedOutputStream bout = new BufferedOutputStream(out)) {
        bout.write(data);
        bout.flush();
      }
    }
  }

  @State(Scope.Benchmark)
  public static class PagedStoreParams extends BlockStoreParams {
    @Param({"1MB", "4MB", "8MB"})
    public String mPageSize;

    @Override
    @Setup(Level.Trial)
    public void setup() throws Exception {
      Configuration.set(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, mPageSize);
      super.setup();
    }
  }

  @Benchmark
  public void monoBlockStoreReadLocal(BlockStoreParams params) throws Exception {
    readFullyLocal(params.mBlockStoreBase.mMonoBlockStore,
        params.mLocalBlockId, params.mBlockSizeByte);
  }

  @Benchmark
  public void monoBlockStoreTransferLocal(BlockStoreParams params) throws Exception {
    transferFullyLocal(params.mBlockStoreBase.mMonoBlockStore,
        params.mLocalBlockId, params.mBlockSizeByte);
  }

  /**
   * Use {@link BlockReader#read} to read all block cached locally to memory.
   * This method simulates {@link alluxio.worker.grpc.BlockReadHandler}'s use of BlockStore
   * when pooling is not enabled.
   *
   * @param store the block store
   * @param blockId the id of the block
   * @param blockSize block size
   * @throws Exception if error occurs
   */
  private void readFullyLocal(BlockStore store, long blockId, long blockSize)
      throws Exception {
    try (BlockReader reader = store
        .createBlockReader(2L, blockId, 0, false,
            Protocol.OpenUfsBlockOptions.newBuilder().build())) {
      ByteBuffer buffer = reader.read(0, blockSize);
      ByteBuf buf = Unpooled.wrappedBuffer(buffer);
      buf.readBytes(SINK, 0, (int) blockSize);
      buf.release();
    }
  }

  /**
   * Use {@link BlockReader#transferTo} to read all block cached locally to memory.
   * This method simulates {@link alluxio.worker.grpc.BlockReadHandler}'s use of BlockStore
   * when pooling is enabled.
   *
   * @param store the block store
   * @param blockId the id of the block
   * @param blockSize block size
   * @throws Exception if error occurs
   */
  private void transferFullyLocal(BlockStore store, long blockId, long blockSize)
      throws Exception {
    try (BlockReader reader = store
        .createBlockReader(2L, blockId, 0, false,
            Protocol.OpenUfsBlockOptions.newBuilder().build())) {
      ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer((int) blockSize, (int) blockSize);
      reader.transferTo(buf);
      buf.readBytes(SINK, 0, (int) blockSize);
      buf.release();
    }
  }

  @Benchmark
  public void monoBlockStoreReadUfs(BlockStoreParams params) throws Exception {
    readFullyUfs(params.mBlockStoreBase.mMonoBlockStore, params.mUfsBlockId,
        params.mUfsMountId, params.mUfsPath, params.mBlockSizeByte);
  }

  @Benchmark
  public void pagedBlockStoreReadUfs(PagedStoreParams params) throws Exception {
    readFullyUfs(params.mBlockStoreBase.mPagedBlockStore, params.mUfsBlockId,
        params.mUfsMountId, params.mUfsPath, params.mBlockSizeByte);
  }

  @Benchmark
  public void monoBlockStoreTransferUfs(BlockStoreParams params) throws Exception {
    transferFullyUfs(params.mBlockStoreBase.mMonoBlockStore, params.mUfsBlockId,
            params.mUfsMountId, params.mUfsPath, params.mBlockSizeByte);
  }

  @Benchmark
  public void pagedBlockStoreTransferUfs(PagedStoreParams params) throws Exception {
    transferFullyUfs(params.mBlockStoreBase.mPagedBlockStore, params.mUfsBlockId,
            params.mUfsMountId, params.mUfsPath, params.mBlockSizeByte);
  }

  /**
   * Use {@link BlockReader#read} to read a block in ufs to memory. Doesn't perform
   * extra caching.
   *
   * @param store the store
   * @param blockId the id of the block
   * @param mountId ufs mount id
   * @param ufsPath ufs file path
   * @param blockSize ufs block size
   * @throws Exception if any error occurs
   */
  private void readFullyUfs(BlockStore store, long blockId, long mountId,
                                  String ufsPath, long blockSize) throws Exception {
    try (BlockReader reader = store
        .createBlockReader(2L, blockId, 0, false,
            Protocol.OpenUfsBlockOptions
                .newBuilder()
                .setNoCache(true)
                .setMaxUfsReadConcurrency(1)
                .setUfsPath(ufsPath)
                .setMountId(mountId)
                .setBlockSize(blockSize)
                .build())) {

      ByteBuffer buffer = reader.read(0, blockSize);
      ByteBuf buf = Unpooled.wrappedBuffer(buffer);
      buf.readBytes(SINK, 0, (int) blockSize);
      buf.release();
    }
  }

  /**
   * Use {@link BlockReader#transferTo} to read a block in ufs to memory. Doesn't perform
   * extra caching.
   *
   * @param store the store
   * @param blockId the id of the block
   * @param mountId ufs mount id
   * @param ufsPath ufs file path
   * @param blockSize ufs block size
   * @throws Exception if any error occurs
   */
  private void transferFullyUfs(BlockStore store, long blockId, long mountId,
                                String ufsPath, long blockSize) throws Exception {
    try (BlockReader reader = store
        .createBlockReader(2L, blockId, 0, false,
            Protocol.OpenUfsBlockOptions
            .newBuilder()
            .setNoCache(true)
            .setMaxUfsReadConcurrency(1)
            .setUfsPath(ufsPath)
            .setMountId(mountId)
            .setBlockSize(blockSize)
            .build())) {

      ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer((int) blockSize, (int) blockSize);
      reader.transferTo(buf);
      buf.readBytes(SINK, 0, (int) blockSize);
      buf.release();
    }
  }

  public static void main() throws RunnerException {
    Options opt =
        new OptionsBuilder().include(BlockStoreReadBench.class.getSimpleName()).addProfiler(
            StackProfiler.class).forks(1).build();
    new Runner(opt).run();
  }
}
