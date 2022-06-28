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

import static java.util.Objects.requireNonNull;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.ChannelType;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.WaitForOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NettyUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.WebServer;
import alluxio.web.WorkerWebServer;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.DefaultBlockWorker;
import alluxio.worker.grpc.GrpcDataServer;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Named;

/**
 * This class encapsulates the different worker services that are configured to run.
 */
@NotThreadSafe
public final class AlluxioWorkerProcess implements WorkerProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioWorkerProcess.class);

  private final TieredIdentity mTieredIdentitiy;

  /** Server for data requests and responses. */
  private final DataServer mDataServer;

  /** If started (i.e. not null), this server is used to serve local data transfer. */
  private DataServer mDomainSocketDataServer;

  /** The worker registry. */
  private final WorkerRegistry mRegistry;

  /** Worker Web UI server. */
  private final WebServer mWebServer;

  /** Used for auto binding. **/
  private ServerSocket mBindSocket;

  /** The bind address for the rpc server. */
  private final InetSocketAddress mRpcBindAddress;

  /** The connect address for the rpc server. */
  private final InetSocketAddress mRpcConnectAddress;

  /** Worker start time in milliseconds. */
  private final long mStartTimeMs;

  /** The manager for all ufs. */
  private final UfsManager mUfsManager;

  /** The jvm monitor.*/
  private JvmPauseMonitor mJvmPauseMonitor;

  /**
   * Creates a new instance of {@link AlluxioWorkerProcess}.
   * @param tieredIdentity
   */
  @Inject
  AlluxioWorkerProcess(
      TieredIdentity tieredIdentity,
      WorkerRegistry workerRegistry,
      UfsManager ufsManager,
      WorkerFactory workerFactory,
      @Named("GrpcBindAddress") InetSocketAddress gRpcBindAddress,
      @Named("GrpcConnectAddress") InetSocketAddress gRpcConnectAddress,
      DataServerFactory dataServerFactory) {
    mTieredIdentitiy = requireNonNull(tieredIdentity);
    mUfsManager = requireNonNull(ufsManager);
    mRegistry = requireNonNull(workerRegistry);
    mRpcBindAddress = requireNonNull(gRpcBindAddress);
    mRpcConnectAddress = requireNonNull(gRpcConnectAddress);
    mStartTimeMs = System.currentTimeMillis();
    List<Callable<Void>> callables = ImmutableList.of(() -> {
      mRegistry.add(BlockWorker.class, workerFactory.create());
      return null;
    });
    try {
      CommonUtils.invokeAll(callables,
          Configuration.getMs(PropertyKey.WORKER_STARTUP_TIMEOUT));
    } catch (TimeoutException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    // Setup web server
    mWebServer =
        new WorkerWebServer(NetworkAddressUtils.getBindAddress(ServiceType.WORKER_WEB,
            Configuration.global()), this,
            mRegistry.get(BlockWorker.class));

    // Setup Data server
    mDataServer = dataServerFactory.createRemoteDataServer(
        (DefaultBlockWorker) mRegistry.get(BlockWorker.class));

    // Setup domain socket data server
    if (isDomainSocketEnabled()) {
      dataServerFactory.createDomainSocketDataServer(
          (DefaultBlockWorker) mRegistry.get(BlockWorker.class));
    }
  }

  @Override
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  @Override
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  @Override
  public String getDataBindHost() {
    return ((InetSocketAddress) mDataServer.getBindAddress()).getHostString();
  }

  @Override
  public int getDataLocalPort() {
    return ((InetSocketAddress) mDataServer.getBindAddress()).getPort();
  }

  @Override
  public String getDataDomainSocketPath() {
    if (mDomainSocketDataServer != null) {
      return ((DomainSocketAddress) mDomainSocketDataServer.getBindAddress()).path();
    }
    return "";
  }

  @Override
  public String getWebBindHost() {
    return mWebServer.getBindHost();
  }

  @Override
  public int getWebLocalPort() {
    return mWebServer.getLocalPort();
  }

  @Override
  public <T extends Worker> T getWorker(Class<T> clazz) {
    return mRegistry.get(clazz);
  }

  @Override
  public UfsManager getUfsManager() {
    return mUfsManager;
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcBindAddress;
  }

  @Override
  public void start() throws Exception {
    // NOTE: the order to start different services is sensitive. If you change it, do it cautiously.

    // Start serving metrics system, this will not block
    MetricsSystem.startSinks(Configuration.getString(PropertyKey.METRICS_CONF_FILE));

    // Start each worker. This must be done before starting the web or RPC servers.
    // Requirement: NetAddress set in WorkerContext, so block worker can initialize BlockMasterSync
    // Consequence: worker id is granted
    startWorkers();

    // Start serving the web server, this will not block.
    mWebServer.start();

    // Start monitor jvm
    if (Configuration.getBoolean(PropertyKey.WORKER_JVM_MONITOR_ENABLED)) {
      mJvmPauseMonitor =
          new JvmPauseMonitor(
              Configuration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS),
              Configuration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS),
              Configuration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS));
      mJvmPauseMonitor.start();
      MetricsSystem.registerGaugeIfAbsent(
              MetricsSystem.getMetricName(MetricKey.TOTAL_EXTRA_TIME.getName()),
              mJvmPauseMonitor::getTotalExtraTime);
      MetricsSystem.registerGaugeIfAbsent(
              MetricsSystem.getMetricName(MetricKey.INFO_TIME_EXCEEDED.getName()),
              mJvmPauseMonitor::getInfoTimeExceeded);
      MetricsSystem.registerGaugeIfAbsent(
              MetricsSystem.getMetricName(MetricKey.WARN_TIME_EXCEEDED.getName()),
              mJvmPauseMonitor::getWarnTimeExceeded);
    }

    // Start serving RPC, this will block
    LOG.info("Alluxio worker started. id={}, bindHost={}, connectHost={}, rpcPort={}, webPort={}",
        mRegistry.get(BlockWorker.class).getWorkerId(),
        NetworkAddressUtils.getBindHost(ServiceType.WORKER_RPC, Configuration.global()),
        NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC, Configuration.global()),
        NetworkAddressUtils.getPort(ServiceType.WORKER_RPC, Configuration.global()),
        NetworkAddressUtils.getPort(ServiceType.WORKER_WEB, Configuration.global()));

    mDataServer.awaitTermination();

    LOG.info("Alluxio worker ended");
  }

  @Override
  public void stop() throws Exception {
    if (isServing()) {
      stopServing();
      if (mJvmPauseMonitor != null) {
        mJvmPauseMonitor.stop();
      }
    }
    stopWorkers();
  }

  private boolean isServing() {
    return mDataServer != null && !mDataServer.isClosed();
  }

  private void startWorkers() throws Exception {
    mRegistry.start(getAddress());
  }

  private void stopWorkers() throws Exception {
    mRegistry.stop();
  }

  private void stopServing() throws Exception {
    mDataServer.close();
    if (mDomainSocketDataServer != null) {
      mDomainSocketDataServer.close();
      mDomainSocketDataServer = null;
    }
    mUfsManager.close();
    try {
      mWebServer.stop();
    } catch (Exception e) {
      LOG.error("Failed to stop {} web server", this, e);
    }
    MetricsSystem.stopSinks();
  }

  /**
   * @return true if domain socket is enabled
   */
  private boolean isDomainSocketEnabled() {
    return NettyUtils.getWorkerChannel(Configuration.global()) == ChannelType.EPOLL
        && Configuration.isSet(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS);
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start",
          () -> isServing() && mRegistry.get(BlockWorker.class).getWorkerId() != null
              && mWebServer != null && mWebServer.getServer().isRunning(),
          WaitForOptions.defaults().setTimeoutMs(timeoutMs));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }

  @Override
  public WorkerNetAddress getAddress() {
    return new WorkerNetAddress()
        .setHost(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC,
            Configuration.global()))
        .setContainerHost(Configuration.global()
            .getOrDefault(PropertyKey.WORKER_CONTAINER_HOSTNAME, ""))
        .setRpcPort(mRpcBindAddress.getPort())
        .setDataPort(getDataLocalPort())
        .setDomainSocketPath(getDataDomainSocketPath())
        .setWebPort(mWebServer.getLocalPort())
        .setTieredIdentity(mTieredIdentitiy);
  }

  @Override
  public String toString() {
    return "Alluxio worker @" + mRpcConnectAddress;
  }
}
