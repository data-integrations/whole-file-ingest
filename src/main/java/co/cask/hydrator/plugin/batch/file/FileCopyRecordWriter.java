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

package co.cask.hydrator.plugin.batch.file;

import co.cask.hydrator.plugin.batch.file.ftp.FTPFileMetadata;
import co.cask.hydrator.plugin.batch.file.ftp.FTPMetadataInputFormat;
import co.cask.hydrator.plugin.batch.file.s3.S3FileMetadata;
import co.cask.hydrator.plugin.batch.file.s3.S3MetadataInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The record writer that takes file metadata and streams data from source database
 * to destination database
 */
public class FileCopyRecordWriter extends RecordWriter<NullWritable, FileMetadata> {
  private final FileSystem destFileSystem;
  private final String basePath;
  private final boolean enableOverwrite;
  private final boolean preserveOwner;
  private final int bufferSize;

  // buffer size defaults to 1 MB
  public static final int DEFAULT_BUFFER_SIZE = 1 << 20;
  private static final Logger LOG = LoggerFactory.getLogger(FileCopyRecordWriter.class);

  // a Key-Value map from host uri to Filesystem object
  private Map<String, FileSystem> sourceFilesystemMap;

  /**
   * Construct a RecordWriter given user configurations.
   *
   * @param conf The configuration that contains required information to intialize the recordWriter.
   * @throws IOException
   */
  public FileCopyRecordWriter(Configuration conf) throws IOException {
    // always disable caching when obtaining destination filesystem
    conf.set(String.format("fs.%s.impl.disable.cache", conf.get(FileCopyOutputFormat.FS_SCHEME)), String.valueOf(true));

    // connect to destination filesystem with uri if it is provided
    String uriString = conf.get(FileCopyOutputFormat.FS_HOST_URI, null);
    if (uriString != null) {
      destFileSystem = FileSystem.get(URI.create(uriString), conf);
    } else {
      destFileSystem = FileSystem.get(conf);
    }

    // initialize other properties for writing to destination filesystem
    basePath = conf.get(FileCopyOutputFormat.BASE_PATH);
    enableOverwrite = conf.getBoolean(FileCopyOutputFormat.ENABLE_OVERWRITE, false);
    preserveOwner = conf.getBoolean(FileCopyOutputFormat.PRESERVE_OWNER, false);
    bufferSize = conf.getInt(FileCopyOutputFormat.BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    sourceFilesystemMap = new HashMap<>();
  }

  /**
   * This method connects to the source filesystem and copies the file specified by the FileMetadata input to the
   * destination filesystem.
   *
   * @param key Unused key.
   * @param fileMetadata Contains metadata for the file we wish to copy.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void write(NullWritable key, FileMetadata fileMetadata) throws IOException, InterruptedException {

    if (fileMetadata.getRelativePath().isEmpty()) {
      // nothing to create
      return;
    }

    // construct file paths for source and destination
    Path srcPath = new Path(fileMetadata.getFullPath());
    Path destPath = new Path(basePath, fileMetadata.getRelativePath());
    FsPermission permission = new FsPermission(fileMetadata.getPermission());

    // immediately return if we don't want to overwrite and file exists in destination
    if (!enableOverwrite && destFileSystem.exists(destPath)) {
      return;
    }

    // get source database connection
    String uriString = fileMetadata.getHostURI();
    if (!sourceFilesystemMap.containsKey(uriString)) {
      sourceFilesystemMap.put(uriString, getSourceFilesystemConnection(fileMetadata));
    }
    FileSystem sourceFilesystem = sourceFilesystemMap.get(uriString);

    // do some checks to see if we need to copy the file
    if (fileMetadata.isDir()) {
      // create an empty directory and return
      if (!destFileSystem.exists(destPath) && sourceFilesystem.isDirectory(srcPath)) {
        destFileSystem.mkdirs(destPath, permission);
        if (preserveOwner) {
          destFileSystem.setOwner(destPath, fileMetadata.getOwner(), fileMetadata.getGroup());
        }
      }
      return;
    } else if (!sourceFilesystem.exists(srcPath)) {
      // file doesn't exist in source, return immediately
      LOG.warn("{} doesn't exist in source filesystem.", fileMetadata.getFullPath());
      return;
    }

    // data streaming
    FSDataInputStream inputStream = sourceFilesystem.open(srcPath, bufferSize);
    FSDataOutputStream outputStream = FileSystem.create(destFileSystem, destPath, permission);
    try {
      byte[] buf = new byte[bufferSize];
      int len;
      while ((len = inputStream.read(buf)) >= 0) {
        outputStream.write(buf, 0, len);
      }
    } finally {
      // we have to do this to make sure even if one stream fails to close, it
      // still attempts to close the other stream
      try {
        inputStream.close();
      } finally {
        outputStream.close();
        // the owner is set only if the output stream is sucessfully closed
        if (preserveOwner) {
          destFileSystem.setOwner(destPath, fileMetadata.getOwner(), fileMetadata.getGroup());
        }
      }
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    // attempts to close the other even if one fails
    try {
      destFileSystem.close();
    } finally {
      safelyCloseSourceFilesystems(sourceFilesystemMap.values().iterator());
    }
  }

  /**
   * this method attempts to close every Filesystem object in the list, logs a warning
   * for each object that fails to close
   *
   * @param fs The iterator over all the Filesystems we wish to close.
   */
  private void safelyCloseSourceFilesystems(Iterator<FileSystem> fs) {
    while (fs.hasNext()) {
      try {
        fs.next().close();
      } catch (IOException e) {
        LOG.warn(e.getMessage());
      }
    }
  }

  /**
   * This method identifies the source filesystem given FileMetadata and returns a FileSystem instance that will be used
   * to read files from the source.
   *
   * @param metadata Contains the metadata of the file we wish to copy
   * @return A FileSystem instance used to communicate with the source filesystem.
   * @throws IOException
   */
  private FileSystem getSourceFilesystemConnection(FileMetadata metadata)
    throws IOException {
    Configuration conf = new Configuration(false);
    conf.clear();

    // always disable caching for source Filesystem objects
    URI uri = URI.create(metadata.getHostURI());
    String disableCacheName = String.format("fs.%s.impl.disable.cache", uri.getScheme());
    conf.set(disableCacheName, String.valueOf(true));

    switch (uri.getScheme()) {
      case "s3a":
        S3FileMetadata s3aFileMetadata = (S3FileMetadata) metadata;
        S3MetadataInputFormat.setS3aAccessKeyId(conf, s3aFileMetadata.getAccessKeyId());
        S3MetadataInputFormat.setS3aSecretKeyId(conf, s3aFileMetadata.getSecretKeyId());
        S3MetadataInputFormat.setS3aFsClass(conf);
        break;
      case "s3n":
        S3FileMetadata s3nFileMetadata = (S3FileMetadata) metadata;
        S3MetadataInputFormat.setS3nAccessKeyId(conf, s3nFileMetadata.getAccessKeyId());
        S3MetadataInputFormat.setS3nSecretKeyId(conf, s3nFileMetadata.getSecretKeyId());
        S3MetadataInputFormat.setS3nFsClass(conf);
        break;
      case "ftp":
        FTPFileMetadata ftpFileMetadata = (FTPFileMetadata) metadata;
        FTPMetadataInputFormat.setFTPUsername(conf, uri.getHost(), ftpFileMetadata.getFtpUsername());
        FTPMetadataInputFormat.setFTPHost(conf, uri.getHost());
        FTPMetadataInputFormat.setFTPFsClass(conf);
        if (ftpFileMetadata.getFtpPassword() != null) {
          FTPMetadataInputFormat.setFTPassword(conf, uri.getHost(), ftpFileMetadata.getFtpPassword());
        }
        break;
      case "sftp":
        FTPFileMetadata sftpFileMetadata = (FTPFileMetadata) metadata;
        FTPMetadataInputFormat.setSFTPUsername(conf, uri.getHost(), sftpFileMetadata.getFtpUsername());
        FTPMetadataInputFormat.setSFTPHost(conf, uri.getHost());
        FTPMetadataInputFormat.setSFTPFsClass(conf);
        if (sftpFileMetadata.getFtpPassword() != null) {
          FTPMetadataInputFormat.setSFTPPassword(conf, uri.getHost(), sftpFileMetadata.getFtpPassword());
        }
        if (sftpFileMetadata.getSftpKeyPath() != null) {
          FTPMetadataInputFormat.setSFTPKeyFilePath(conf, sftpFileMetadata.getSftpKeyPath());
        }
        break;
      case "file":
      case "hdfs":
        // TODO: figure out how to read from/write to remote HDFS. Currently this only supports writing to local HDFS.
        conf = new Configuration();
        break;
      default:
        throw new IOException(uri.getScheme() + " is not supported.");
    }

    return FileSystem.get(uri, conf);
  }
}
