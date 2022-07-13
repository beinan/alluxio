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

package alluxio.client.file.cache.limiter;

import alluxio.conf.AlluxioConfiguration;

/**
 * Noop write limiter.
 */
public class NoopWriteLimiter implements WriteLimiter {
  /**
   * Constructor for NoopWriteLimiter.
   * @param conf
   */
  public NoopWriteLimiter(AlluxioConfiguration conf) {
  }

  @Override
  public boolean tryAcquire(int writeLength) {
    return true;
  }
}
