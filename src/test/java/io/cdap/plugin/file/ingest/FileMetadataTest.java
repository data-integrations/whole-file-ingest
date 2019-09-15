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

package io.cdap.plugin.file.ingest;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FileMetadataTest {
  @Test
  public void testRelativePathParsing() throws IOException {
    FileStatus fileStatus = new FileStatus();
    fileStatus.setPath(new Path("hdfs://12.34.56.78/source/path/directory/123.txt"));

    // Copy a file that is part of a whole directory copy
    String sourcePath = "/source/path/directory";
    FileMetadata metadata = new FileMetadata(fileStatus, sourcePath);
    Assert.assertEquals("123.txt", metadata.getFileName());
    Assert.assertEquals("/source/path/directory/123.txt", metadata.getFullPath());
    Assert.assertEquals("directory/123.txt", metadata.getRelativePath());
    Assert.assertEquals("hdfs://12.34.56.78/", metadata.getHostURI());

    // Copy a file that is part of a whole directory copy without including the directory
    sourcePath = "/source/path/";
    metadata = new FileMetadata(fileStatus, sourcePath);
    Assert.assertEquals("123.txt", metadata.getFileName());
    Assert.assertEquals("/source/path/directory/123.txt", metadata.getFullPath());
    Assert.assertEquals("directory/123.txt", metadata.getRelativePath());
    Assert.assertEquals("hdfs://12.34.56.78/", metadata.getHostURI());

    fileStatus.setPath(new Path("hdfs://12.34.56.78/"));
    sourcePath = "/";
    metadata = new FileMetadata(fileStatus, sourcePath);
    Assert.assertTrue(metadata.getRelativePath().isEmpty());

    fileStatus.setPath(new Path("hdfs://12.34.56.78/abc.txt"));
    sourcePath = "/";
    metadata = new FileMetadata(fileStatus, sourcePath);
    Assert.assertEquals("abc.txt", metadata.getRelativePath());
  }

  @Test
  public void testCompare() throws IOException {
    final FileStatus statusA = new FileStatus(1, false, 0, 0, 0, new Path("s3a://hello.com/abc/fileA"));
    final FileStatus statusB = new FileStatus(3, false, 0, 0, 0, new Path("s3a://hello.com/abc/fileB"));
    final FileStatus statusC = new FileStatus(3, false, 0, 0, 0, new Path("s3a://hello.com/abc/fileC"));
    final String basePath = "/abc";

    // generate 3 files with different file sizes
    FileMetadata file1 = new FileMetadata(statusA, basePath);

    FileMetadata file2 = new FileMetadata(statusB, basePath);

    FileMetadata file3 = new FileMetadata(statusC, basePath);

    Assert.assertEquals(-1, file1.compareTo(file2));
    Assert.assertEquals(0, file3.compareTo(file2));
    Assert.assertEquals(1, file3.compareTo(file1));
  }
}
