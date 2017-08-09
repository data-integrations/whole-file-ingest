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

import co.cask.hydrator.plugin.batch.file.FileMetadata;
import co.cask.hydrator.plugin.batch.file.MetadataInputFormat;
import co.cask.hydrator.plugin.batch.file.MetadataInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.fs.sftp.SFTPFileSystem;

import java.io.IOException;

public class FTPMetadataInputFormat extends MetadataInputFormat {

  // configs for ftp
  public static final String FTP_FS_CLASS = "fs.ftp.impl";

  // configs for sftp
  public static final String SFTP_FS_CLASS = "fs.sftp.impl";

  // setters for FTP
  public static void setFTPUsername(Configuration conf, String host, String username) {
    conf.set(FTPFileSystem.FS_FTP_USER_PREFIX + host, username);
  }

  public static void setFTPassword(Configuration conf, String host, String password) {
    conf.set(FTPFileSystem.FS_FTP_PASSWORD_PREFIX + host, password);
  }

  public static void setFTPHost(Configuration conf, String host) {
    conf.set(FTPFileSystem.FS_FTP_HOST, host);
  }

  public static void setFTPFsClass(Configuration conf) {
    conf.set(FTP_FS_CLASS, FTPFileSystem.class.getName());
  }

  // setters for SFTP
  public static void setSFTPUsername(Configuration conf, String host, String username) {
    conf.set(SFTPFileSystem.FS_SFTP_USER_PREFIX + host, username);
  }

  public static void setSFTPPassword(Configuration conf, String host, String password) {
    conf.set(SFTPFileSystem.FS_SFTP_PASSWORD_PREFIX + host, password);
  }

  public static void setSFTPHost(Configuration conf, String host) {
    conf.set(SFTPFileSystem.FS_SFTP_HOST, host);
  }

  public static void setSFTPKeyFilePath(Configuration conf, String path) {
    conf.set(SFTPFileSystem.FS_SFTP_KEYFILE, path);
  }

  public static void setSFTPFsClass(Configuration conf) {
    conf.set(SFTP_FS_CLASS, SFTPFileSystem.class.getName());
  }

  @Override
  protected MetadataInputSplit getInputSplit() {
    return new FTPMetadataInputSplit();
  }

  @Override
  protected FileMetadata getFileMetadata(FileStatus fileStatus, String sourcePath, Configuration conf)
    throws IOException {
    String host = fileStatus.getPath().toUri().getHost();
    switch (fileStatus.getPath().toUri().getScheme()) {
      case "ftp":
        return new FTPFileMetadata(fileStatus, sourcePath,
                                   conf.get(FTPFileSystem.FS_FTP_USER_PREFIX + host),
                                   conf.get(FTPFileSystem.FS_FTP_PASSWORD_PREFIX + host, null),
                                   null);
      case "sftp":
        return new FTPFileMetadata(fileStatus, sourcePath,
                                   conf.get(SFTPFileSystem.FS_SFTP_USER_PREFIX + host),
                                   conf.get(SFTPFileSystem.FS_SFTP_PASSWORD_PREFIX + host, null),
                                   conf.get(SFTPFileSystem.FS_SFTP_KEYFILE, null));
      default:
        throw new IOException("Scheme must be either ftp or sftp");
    }
  }
}
