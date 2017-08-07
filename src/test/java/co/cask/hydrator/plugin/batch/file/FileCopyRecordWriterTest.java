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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FileCopyRecordWriterTest {

  private File sourceDir;
  private MiniDFSCluster sourceCluster;
  private FileSystem sourceFS;

  private File destDir;
  private MiniDFSCluster destCluster;
  private FileSystem destFS;

  @Before
  public void setUp() throws IOException {
    sourceDir = Files.createTempDirectory("temp-source").toFile().getAbsoluteFile();
    destDir = Files.createTempDirectory("temp-dest").toFile().getAbsoluteFile();

    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, sourceDir.getAbsolutePath());
    sourceCluster = new MiniDFSCluster.Builder(conf).build();
    sourceFS = sourceCluster.getFileSystem();

    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, destDir.getAbsolutePath());
    destCluster = new MiniDFSCluster.Builder(conf).build();
    destFS = destCluster.getFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    try {
      sourceFS.close();
      sourceCluster.shutdown();
      destFS.close();
      destCluster.shutdown();
    } finally {
      // we should always delete the temporary directory that was created
      FileUtil.fullyDelete(sourceDir);
      FileUtil.fullyDelete(destDir);
    }
  }

  @Test
  public void copyTest() {

  }
}
