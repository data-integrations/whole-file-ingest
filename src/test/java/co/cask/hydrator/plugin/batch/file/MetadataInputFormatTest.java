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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MetadataInputFormatTest {

  private File testDir;
  private MiniDFSCluster cluster;
  private FileSystem fileSystem;

  @Before
  public void setUp() throws IOException {
    testDir = Files.createTempDirectory("whole-file-ingest-test").toFile().getAbsoluteFile();
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDir.getAbsolutePath());
    cluster = new MiniDFSCluster.Builder(conf).build();
    fileSystem = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    try {
      wipeFS();
      fileSystem.close();
      cluster.shutdown();
    } finally {
      // we should always delete the temporary directory that was created
      FileUtil.fullyDelete(testDir);
    }
  }

  @Test
  public void getSplitTest() throws IOException, InterruptedException {
    // initialize configuration
    Configuration conf = fileSystem.getConf();
    MetadataInputFormat.setRecursiveCopy(conf, String.valueOf(true));

    MetadataInputFormat.setSourcePaths(conf, "/");

    // Add files to the filesystem
    // folderA
    Path folderA = new Path("/folderA");
    createFileBySize(new Path(folderA, "file.100"), 100);
    createFileBySize(new Path(folderA, "file.2"), 2);
    createFileBySize(new Path(folderA, "file.3"), 3);

    // folderB
    Path folderB = new Path("/folderB");
    createFileBySize(new Path(folderB, "file.1"), 1);
    createFileBySize(new Path(folderB, "file.200"), 200);
    createFileBySize(new Path(folderB, "file.300"), 300);

    // test when max split size is 8
    MetadataInputFormat.setMaxSplitSize(conf, 8);
    MetadataInputFormat inputFormat = new MetadataInputFormat();
    JobContext jobContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    List<MetadataInputSplit> splits = convertToMetadataInputSplits(inputFormat.getSplits(jobContext));
    Assert.assertEquals(1, splits.size());
    Assert.assertEquals(606, splits.get(0).getTotalBytes());

    // test when max split size is 7
    MetadataInputFormat.setMaxSplitSize(conf, 7);
    jobContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    splits = convertToMetadataInputSplits(inputFormat.getSplits(jobContext));
    Collections.sort(splits);
    Assert.assertEquals(2, splits.size());
    Assert.assertEquals(303, splits.get(0).getTotalBytes());
    Assert.assertEquals(303, splits.get(1).getTotalBytes());

    // test when max split size is 4
    MetadataInputFormat.setMaxSplitSize(conf, 4);
    jobContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    splits = convertToMetadataInputSplits(inputFormat.getSplits(jobContext));
    Collections.sort(splits);
    Assert.assertEquals(2, splits.size());
    Assert.assertEquals(303, splits.get(0).getTotalBytes());
    Assert.assertEquals(303, splits.get(1).getTotalBytes());

    // test when max split size is 3
    MetadataInputFormat.setMaxSplitSize(conf, 3);
    jobContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    splits = convertToMetadataInputSplits(inputFormat.getSplits(jobContext));
    Collections.sort(splits);
    Assert.assertEquals(3, splits.size());
    Assert.assertEquals(105, splits.get(0).getTotalBytes());
    Assert.assertEquals(201, splits.get(1).getTotalBytes());
    Assert.assertEquals(300, splits.get(2).getTotalBytes());

    // test when max split size is 2
    MetadataInputFormat.setMaxSplitSize(conf, 2);
    jobContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    splits = convertToMetadataInputSplits(inputFormat.getSplits(jobContext));
    Collections.sort(splits);
    Assert.assertEquals(4, splits.size());
    Assert.assertEquals(5, splits.get(0).getTotalBytes());
    Assert.assertEquals(101, splits.get(1).getTotalBytes());
    Assert.assertEquals(200, splits.get(2).getTotalBytes());
    Assert.assertEquals(300, splits.get(3).getTotalBytes());

    // always wipe after a test
    wipeFS();
  }

  private void createFileBySize(Path path, int size) throws IOException {
    FSDataOutputStream outputStream = fileSystem.create(path, true);
    byte[] buf = new byte[size];
    outputStream.write(buf, 0, size);
    outputStream.close();
  }

  private List<MetadataInputSplit> convertToMetadataInputSplits(List<InputSplit> splits) {
    List<MetadataInputSplit> metadataInputSplits = new ArrayList<>();
    for (InputSplit split : splits) {
      metadataInputSplits.add((MetadataInputSplit) split);
    }
    return metadataInputSplits;
  }

  private void wipeFS() throws IOException {
    fileSystem.delete(new Path("/"), true);
  }
}