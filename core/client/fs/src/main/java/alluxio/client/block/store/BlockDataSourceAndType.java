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

package alluxio.client.block.store;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.util.BlockLocationUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 *
 */
public class BlockDataSourceAndType {
  private static final Logger LOG = LoggerFactory.getLogger(BlockDataSourceAndType.class);

  private final FileSystemContext mContext;
  private final TieredIdentity mTieredIdentity;

  /**
   * Construct BlockDataSourceAndType
   *
   * @param context the file system context
   * @param tieredIdentity the tiered identity
   */
  BlockDataSourceAndType(FileSystemContext context,
                         TieredIdentity tieredIdentity) {
    mContext = context;
    mTieredIdentity = tieredIdentity;
  }

  /**
   * Gets the data source and type of data source of a block. This method is primarily responsible
   * for determining the data source and type of data source. It takes a map of failed workers and
   * their most recently failed time and tries to update it when BlockInStream created failed,
   * attempting to avoid reading from a recently failed worker.
   *
   * @param info the info of the block to read
   * @param status the URIStatus associated with the read request
   * @param policy the policy determining the Alluxio worker location
   * @param failedWorkers the map of workers address to most recent failure time
   * @return the data source and type of data source of the block
   */
  public Pair<WorkerNetAddress, BlockInStream.BlockInStreamSource> getDataSourceAndType(
      BlockInfo info,
      URIStatus status, BlockLocationPolicy policy, Map<WorkerNetAddress, Long> failedWorkers)
      throws IOException {
    List<BlockLocation> locations = info.getLocations();
    List<BlockWorkerInfo> blockWorkerInfo = Collections.EMPTY_LIST;
    // Initial target workers to read the block given the block locations.
    Set<WorkerNetAddress> workerPool;
    // Note that, it is possible that the blocks have been written as UFS blocks
    if (status.isPersisted()
        || status.getPersistenceState().equals("TO_BE_PERSISTED")) {
      blockWorkerInfo = mContext.getCachedWorkers();
      if (blockWorkerInfo.isEmpty()) {
        throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
      }
      workerPool = blockWorkerInfo.stream().map(BlockWorkerInfo::getNetAddress).collect(toSet());
    } else {
      if (locations.isEmpty()) {
        blockWorkerInfo = mContext.getCachedWorkers();
        if (blockWorkerInfo.isEmpty()) {
          throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
        }
        throw new UnavailableException(
            ExceptionMessage.BLOCK_UNAVAILABLE.getMessage(info.getBlockId()));
      }
      workerPool = locations.stream().map(BlockLocation::getWorkerAddress).collect(toSet());
    }
    // Workers to read the block, after considering failed workers.
    Set<WorkerNetAddress> workers = handleFailedWorkers(workerPool, failedWorkers);
    // TODO(calvin, jianjian): Consider containing these two variables in one object
    BlockInStream.BlockInStreamSource dataSourceType = null;
    WorkerNetAddress dataSource = null;
    locations = locations.stream()
        .filter(location -> workers.contains(location.getWorkerAddress())).collect(toList());
    // First try to read data from Alluxio
    if (!locations.isEmpty()) {
      // TODO(calvin): Get location via a policy
      List<WorkerNetAddress> tieredLocations =
          locations.stream().map(location -> location.getWorkerAddress())
              .collect(toList());
      Collections.shuffle(tieredLocations);
      Optional<Pair<WorkerNetAddress, Boolean>> nearest =
          BlockLocationUtils.nearest(mTieredIdentity, tieredLocations, mContext.getClusterConf());
      if (nearest.isPresent()) {
        dataSource = nearest.get().getFirst();
        dataSourceType = nearest.get().getSecond() ? mContext.hasProcessLocalWorker()
            ? BlockInStream.BlockInStreamSource.PROCESS_LOCAL : BlockInStream.BlockInStreamSource.NODE_LOCAL
            : BlockInStream.BlockInStreamSource.REMOTE;
      }
    }
    // Can't get data from Alluxio, get it from the UFS instead
    if (dataSource == null) {
      dataSourceType = BlockInStream.BlockInStreamSource.UFS;
      Preconditions.checkNotNull(policy,
          PreconditionMessage.UFS_READ_LOCATION_POLICY_UNSPECIFIED);
      blockWorkerInfo = blockWorkerInfo.stream()
          .filter(workerInfo -> workers.contains(workerInfo.getNetAddress())).collect(toList());
      GetWorkerOptions getWorkerOptions = GetWorkerOptions.defaults()
          .setBlockInfo(new BlockInfo()
              .setBlockId(info.getBlockId())
              .setLength(info.getLength())
              .setLocations(locations))
          .setBlockWorkerInfos(blockWorkerInfo);
      dataSource = policy.getWorker(getWorkerOptions);
      if (dataSource != null) {
        if (mContext.hasProcessLocalWorker()
            && dataSource.equals(mContext.getNodeLocalWorker())) {
          dataSourceType = BlockInStream.BlockInStreamSource.PROCESS_LOCAL;
          LOG.debug("Create BlockInStream to read data from UFS through process local worker {}",
              dataSource);
        } else {
          LOG.debug("Create BlockInStream to read data from UFS through worker {} "
                  + "(client embedded in local worker process: {},"
                  + "client co-located with worker in different processes: {}, "
                  + "local worker address: {})",
              dataSource, mContext.hasProcessLocalWorker(), mContext.hasNodeLocalWorker(),
              mContext.hasNodeLocalWorker() ? mContext.getNodeLocalWorker() : "N/A");
        }
      }
    }

    if (dataSource == null) {
      throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }
    return new Pair<>(dataSource, dataSourceType);
  }

  private Set<WorkerNetAddress> handleFailedWorkers(Set<WorkerNetAddress> workers,
                                                    Map<WorkerNetAddress, Long> failedWorkers) {
    if (workers.isEmpty()) {
      return Collections.EMPTY_SET;
    }
    Set<WorkerNetAddress> nonFailed =
        workers.stream().filter(worker -> !failedWorkers.containsKey(worker)).collect(toSet());
    if (nonFailed.isEmpty()) {
      return Collections.singleton(workers.stream()
          .min((x, y) -> Long.compare(failedWorkers.get(x), failedWorkers.get(y))).get());
    }
    return nonFailed;
  }
}
