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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Abstract class that implements the inputFormat for the FileCopySource plugin to
 * read file metadata. The getsplit method creates splits according to user-set configuration
 * and tries to assign files to each split such that every split copies roughly the same number of bytes.
 */
public class MetadataInputFormat extends InputFormat {

  protected static final String SOURCE_PATHS = "source.paths";
  protected static final String MAX_SPLIT_SIZE = "max.split.size";
  protected static final String FS_URI = "filesystem.uri";
  protected static final String RECURSIVE_COPY = "recursive.copy";
  protected static final int DEFAULT_MAX_SPLIT_SIZE = 128;
  private static final Logger LOG = LoggerFactory.getLogger(MetadataInputFormat.class);

  public static void setSourcePaths(Configuration conf, String value) {
    conf.set(SOURCE_PATHS, value);
  }

  public static void setMaxSplitSize(Configuration conf, int value) {
    conf.setInt(MAX_SPLIT_SIZE, value);
  }

  public static void setURI(Configuration conf, String value) {
    conf.set(FS_URI, value);
  }

  public static void setRecursiveCopy(Configuration conf, String value) {
    conf.set(RECURSIVE_COPY, value);
  }

  public MetadataInputFormat() {
    // no op
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    MetadataRecordReader recordReader = new MetadataRecordReader();
    recordReader.initialize(inputSplit, taskAttemptContext);
    return recordReader;
  }

  /**
   * This method scans the files under the directories specified by the user. Splits are created given the total
   * number of files and the number of files each split can contain. The files are then assigned to splits such that
   * each split gets roughly the same number of bytes.
   *
   * @param jobContext Contains the configurations specified by the user.
   * @return The list of splits.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    // connect to the source filesystem
    Configuration conf = jobContext.getConfiguration();
    String[] sourcePaths = conf.get(SOURCE_PATHS).split(",");
    boolean recursive = conf.getBoolean(RECURSIVE_COPY, true);
    int maxSplitSize = conf.getInt(MAX_SPLIT_SIZE, DEFAULT_MAX_SPLIT_SIZE);

    // create Filesystem object
    FileSystem fileSystem;
    if (conf.get(FS_URI) != null) {
      URI uri = URI.create(conf.get(FS_URI));
      fileSystem = FileSystem.get(uri, conf);
    } else {
      // This case applies for local HDFS
      fileSystem = FileSystem.get(conf);
    }

    // scan the directories specified by the user
    List<FileMetadata> fileMetaDataList = new ArrayList<>();
    for (String prefix : sourcePaths) {
      recursivelyAddFileStatus(fileMetaDataList, prefix, new Path(prefix), recursive, fileSystem, conf);
    }

    // sort fileMetadataList in descending order such that total number of bytes can be more evenly distributed
    Collections.sort(fileMetaDataList);
    Collections.reverse(fileMetaDataList);

    // compute number of splits and instantiate the splits
    // We use a priority queue to keep track of the smallest split (fewest bytes assigned to it)
    int numSplits = (fileMetaDataList.size() - 1) / maxSplitSize + 1;
    PriorityQueue<MetadataInputSplit> abstractInputSplits = new PriorityQueue<>(numSplits);
    for (int i = 0; i < numSplits; i++) {
      abstractInputSplits.add(getInputSplit());
    }

    // assign each split approximately the same number of bytes (2-approx)
    List<InputSplit> inputSplits = new ArrayList<>();
    for (FileMetadata fileMetadata : fileMetaDataList) {
      // remove the smallest split from the priority queue and add a new file to it
      MetadataInputSplit minInputSplit = abstractInputSplits.poll();
      minInputSplit.addFileMetadata(fileMetadata);

      // if the inputsplit has number files more than maxSplitSize, we stop adding files to it
      // otherwise we put it back into the priority queue
      if (minInputSplit.getLength() < maxSplitSize) {
        abstractInputSplits.add(minInputSplit);
      } else {
        inputSplits.add(minInputSplit);
      }
    }

    // add the rest of the splits still on the PriorityQueue into the return list
    inputSplits.addAll(abstractInputSplits);

    return inputSplits;
  }

  /**
   * Because the existing Filesystem.listFiles(Path, Boolean) doesn't list empty directories, we
   * added our own method to recursively traverse the file directories. If the path doesn't exist
   * in the source filesystem, it logs a warning and skips the path.
   *
   * @param fileMetadataList The list that contains all the files under fileStatus.getPath
   * @param prefix The user-set path that was used to get this group of files
   * @param path The path of the file that will be inserted into fileMetadataList.
   * @param recursive Whether or not to recursively scan the directories.
   * @param filesystem The filesystem that contains the files.
   * @param conf The configuration that contains credential information needed to connect to the filesystem.
   * @throws IOException
   */
  private void recursivelyAddFileStatus(List<FileMetadata> fileMetadataList,
                                         String prefix, Path path, Boolean recursive,
                                         FileSystem filesystem, Configuration conf) throws IOException {
    try {
      RemoteIterator<LocatedFileStatus> iter = filesystem.listLocatedStatus(path);
      while (iter.hasNext()) {
        LocatedFileStatus fileStatus = iter.next();
        fileMetadataList.add(getFileMetadata(fileStatus, prefix, conf));
        if (fileStatus.isDirectory() && recursive) {
          recursivelyAddFileStatus(fileMetadataList, prefix, fileStatus.getPath(), recursive, filesystem, conf);
        }
      }
    } catch (FileNotFoundException e) {
      // log a warning and skip if the path doesn't exist
      LOG.warn(e.getMessage());
    }
  }

  /**
   * Returns an empty MetadataInputSplit instance. Override this method to return an InputSplit instance that works for
   * your filesystem.
   *
   * @return An empty MetadataInputSplit instance.
   */
  protected MetadataInputSplit getInputSplit() {
    return new MetadataInputSplit();
  }

  /**
   * Returns a FileMetadata given the FileStatus and soucePath. Override ths method to return a FileMetadata that
   * contains additional credentials for your filesystem.
   *
   * @param fileStatus The FileStatus object that contains the raw metadata of the file
   * @param sourcePath The source path specified by the user that was used to obtain this FileMetadata. Will be used to
   *                   construct the relativePath field in FileMetadata.
   * @param conf The user specified configuration that contains additional credentials required for accessing the source
   *             filesystem.
   * @return A FileMetadata instance with its metadata fields populated
   * @throws IOException
   */
  protected FileMetadata getFileMetadata(FileStatus fileStatus, String sourcePath, Configuration conf)
    throws IOException {
    return new FileMetadata(fileStatus, sourcePath);
  }
}
