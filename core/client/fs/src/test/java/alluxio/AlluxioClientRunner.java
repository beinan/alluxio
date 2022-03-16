package alluxio;

import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AlluxioClientRunner {
  public static void main(String[] args) throws IOException, AlluxioException {
    InstancedConfiguration conf = InstancedConfiguration.defaults();
    conf.set(PropertyKey.SECURITY_LOGIN_USERNAME, "beinan");
    FileSystem customizedFs = FileSystem.Factory.create(conf);
    AlluxioURI customizedPath = new AlluxioURI("/customizedFile");
    try (FileOutStream customizedOut = customizedFs.createFile(customizedPath)) {
      customizedOut.write("aaa".getBytes(StandardCharsets.UTF_8));
    }
  }
}
