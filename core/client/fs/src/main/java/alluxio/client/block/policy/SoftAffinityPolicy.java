package alluxio.client.block.policy;

import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.wire.WorkerNetAddress;

import javax.annotation.Nullable;

public class SoftAffinityPolicy implements BlockLocationPolicy{

  @Nullable
  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    return null;
  }
}
