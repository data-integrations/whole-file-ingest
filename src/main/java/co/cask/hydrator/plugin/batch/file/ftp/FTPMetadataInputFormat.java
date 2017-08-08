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

import co.cask.hydrator.plugin.batch.file.MetadataInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.fs.sftp.SFTPFileSystem;

public class FTPMetadataInputFormat extends MetadataInputFormat {

  // configs for ftp
  public static final String FTP_FS_CLASS = "fs.ftp.impl";

  // configs for sftp
  public static final String SFTP_FS_CLASS = "fs.sftp.impl";

  public static void setFtpUsername(Configuration conf, String host, String username) {
    conf.set(FTPFileSystem.FS_FTP_USER_PREFIX + host, username);
  }

  public static void setFtpPassword(Configuration conf, String host, String password) {
    conf.set(FTPFileSystem.FS_FTP_PASSWORD_PREFIX + host, password);
  }

  public static void setFtpHost(Configuration conf, String host) {
    conf.set(FTPFileSystem.FS_FTP_HOST, host);
  }

  public static void setFtpFsClass(Configuration conf) {
    conf.set(FTP_FS_CLASS, FTPFileSystem.class.getName());
  }

  public static void setSftpFsClass(Configuration conf) {
    conf.set(SFTP_FS_CLASS, SFTPFileSystem.class.getName());
  }
}
