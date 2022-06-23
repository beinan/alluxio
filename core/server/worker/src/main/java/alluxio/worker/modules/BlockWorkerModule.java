package alluxio.worker.modules;

import alluxio.ClientContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.network.TieredIdentityFactory;
import alluxio.underfs.UfsManager;
import alluxio.underfs.WorkerUfsManager;
import alluxio.wire.TieredIdentity;
import alluxio.worker.AlluxioWorkerProcess;
import alluxio.worker.WorkerFactory;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.BlockStore;
import alluxio.worker.block.BlockStoreType;
import alluxio.worker.block.BlockWorkerFactory;
import alluxio.worker.block.MonoBlockStore;
import alluxio.worker.block.TieredBlockStore;
import alluxio.worker.file.FileSystemMasterClient;
import alluxio.worker.page.PagedBlockStore;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Guice module for block worker.
 */
public class BlockWorkerModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerModule.class);

  @Override
  protected void configure() {
    bind(TieredIdentity.class).toProvider(() ->
        TieredIdentityFactory.localIdentity(Configuration.global()));
    bind(new TypeLiteral<AtomicReference<Long>>(){}).annotatedWith(Names.named("workerId"))
        .toInstance(new AtomicReference<>(-1L));
    bind(FileSystemMasterClient.class).toProvider(() -> new FileSystemMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(Configuration.global())).build()));
    bind(UfsManager.class).to(WorkerUfsManager.class).in(Scopes.SINGLETON);
    bind(WorkerFactory.class).to(BlockWorkerFactory.class).in(Scopes.SINGLETON);
    bind(WorkerProcess.class).to(AlluxioWorkerProcess.class).in(Scopes.SINGLETON);
  }

  /**
   * @param blockMasterClientPool
   * @param ufsManager
   * @param workerId
   * @return Instance of BlockStore
   */
  @Provides
  public BlockStore provideBlockStore(
      BlockMasterClientPool blockMasterClientPool,
      UfsManager ufsManager,
      @Named("workerId") AtomicReference<Long> workerId
  ) {
    switch (Configuration.global()
        .getEnum(PropertyKey.USER_BLOCK_STORE_TYPE, BlockStoreType.class)) {
      case PAGE:
        LOG.info("Creating PagedBlockWorker");
        return PagedBlockStore.create(ufsManager);
      case FILE:
        LOG.info("Creating DefaultBlockWorker");
        return
            new MonoBlockStore(new TieredBlockStore(), blockMasterClientPool, ufsManager,
                workerId);
      default:
        throw new UnsupportedOperationException("Unsupported block store type.");
    }
  }
}
