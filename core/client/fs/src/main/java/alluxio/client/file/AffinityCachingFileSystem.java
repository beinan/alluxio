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

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.exception.AlluxioException;
import alluxio.grpc.GetStatusPOptions;
import alluxio.wire.FileInfo;

import java.io.IOException;

/**
 * FileSystem implementation for affinity caching.
 */
public class AffinityCachingFileSystem extends BaseFileSystem {
  /**
   * Constructs a new affinity caching file system.
   *
   * @param fsContext file system context
   */
  public AffinityCachingFileSystem(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, final GetStatusPOptions options)
      throws IOException, AlluxioException {
    checkUri(path);
    FileInfo fileInfo = new FileInfo().setUfsPath(path.getPath());
    URIStatus status = new URIStatus(fileInfo);
    return status;
  }
}
