/*
 * Copyright © 2017 Cask Data, Inc.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *  
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.batch.file.ftp;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.plugin.batch.file.AbstractFileCopySink;
import co.cask.hydrator.plugin.batch.file.FileCopyOutputFormat;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.fs.sftp.SFTPFileSystem;

import java.net.URI;
import javax.annotation.Nullable;

/**
 * FileCopySink that writes to FTP.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("FTPFileCopySink")
@Description("Copies files from remote filesystem to FTP Filesystem")
public class FTPFileCopySink extends AbstractFileCopySink {

  private FTPFileCopySinkConfig config;

  public FTPFileCopySink(FTPFileCopySinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    config.validate();
    context.addOutput(Output.of(config.referenceName, new FTPFileCopyOutputFormatProvider(config)));
  }

  public class FTPFileCopySinkConfig extends AbstractFileCopySinkConfig {
    // configurations for FTP
    @Macro
    @Description("The URI of the filesystem")
    public String filesystemURI;

    @Macro
    @Description("FTP username")
    public String username;

    @Macro
    @Nullable
    @Description("FTP password")
    public String password;

    @Macro
    @Nullable
    @Description("FTP key path")
    public String keyPath;

    public FTPFileCopySinkConfig(String name, String basePath, Boolean enableOverwrite, Boolean preserveFileOwner,
                                 @Nullable Integer bufferSize, String filesystemURI, String username,
                                 @Nullable String password, @Nullable String keyPath) {
      super(name, basePath, enableOverwrite, preserveFileOwner, bufferSize);
      this.filesystemURI = filesystemURI;
      this.username = username;
      this.password = password;
      this.keyPath = keyPath;
    }

    @Override
    public String getScheme() {
      return URI.create(filesystemURI).getScheme();
    }
  }

  public class FTPFileCopyOutputFormatProvider extends FileCopyOutputFormatProvider {
    public FTPFileCopyOutputFormatProvider(AbstractFileCopySinkConfig config) {
      super(config);
      FTPFileCopySinkConfig ftpConfig = (FTPFileCopySinkConfig) config;

      FileCopyOutputFormat.setFilesystemHostUri(conf, ftpConfig.filesystemURI);
      URI hostURI = URI.create(ftpConfig.filesystemURI);

      switch (config.getScheme()) {
        case "ftp":
          conf.put(FTPFileSystem.FS_FTP_HOST, hostURI.getHost());
          conf.put(FTPFileSystem.FS_FTP_USER_PREFIX + hostURI.getHost(), ftpConfig.username);
          conf.put(FTPMetadataInputFormat.FTP_FS_CLASS, FTPFileSystem.class.getName());
          if (ftpConfig.password != null) {
            conf.put(FTPFileSystem.FS_FTP_PASSWORD_PREFIX + hostURI.getHost(), ftpConfig.password);
          }
          break;
        case "sftp":
          conf.put(SFTPFileSystem.FS_SFTP_HOST, hostURI.getHost());
          conf.put(SFTPFileSystem.FS_SFTP_USER_PREFIX + hostURI.getHost(), ftpConfig.username);
          conf.put(FTPMetadataInputFormat.FTP_FS_CLASS, FTPFileSystem.class.getName());
          if (ftpConfig.password != null) {
            conf.put(SFTPFileSystem.FS_SFTP_PASSWORD_PREFIX + hostURI.getHost(), ftpConfig.password);
          }
          if (ftpConfig.keyPath != null) {
            conf.put(SFTPFileSystem.FS_SFTP_KEYFILE, ftpConfig.keyPath);
          }
          break;
        default:
          throw new IllegalArgumentException("Scheme must be either ftp or sftp.");
      }
    }
  }
}
