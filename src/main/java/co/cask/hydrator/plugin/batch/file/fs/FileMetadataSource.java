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

package co.cask.hydrator.plugin.batch.file.fs;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.plugin.batch.file.AbstractFileMetadataSource;
import co.cask.hydrator.plugin.batch.file.FileMetadata;
import co.cask.hydrator.plugin.batch.file.MetadataInputFormat;
import co.cask.hydrator.plugin.common.JobUtils;
import co.cask.hydrator.plugin.common.SourceInputFormatProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * FileCopySource plugin that pulls filemetadata from local filesystem or local HDFS.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("FileMetadataSource")
@Description("Reads file metadata from local filesystem or local HDFS.")
public class FileMetadataSource extends AbstractFileMetadataSource<FileMetadata> {
  private FileMetadataSourceConfig config;

  public FileMetadataSource(FileMetadataSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    List<Schema.Field> fieldList = new ArrayList<>(FileMetadata.DEFAULT_SCHEMA.getFields());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.recordOf("fileSchema", fieldList));
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    super.prepareRun(context);
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    // initialize configurations
    setDefaultConf(conf);
    switch (config.scheme) {
      case "file" :
        MetadataInputFormat.setURI(conf, new URI(config.scheme, null, Path.SEPARATOR, null).toString());
        break;
      case "hdfs" :
        break;
      default:
        throw new IllegalArgumentException("Scheme must be either file or hdfs.");
    }

    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(MetadataInputFormat.class, conf)));
  }

  /**
   * Converts the input FileMetadata to a StructuredRecord and emits it.
   *
   * @param input The input FileMetadata.
   * @param emitter Emits StructuredRecord that contains FileMetadata.
   */
  @Override
  public void transform(KeyValue<NullWritable, FileMetadata> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue().toRecord());
  }

  /**
   * Configurations required for connecting to local filesystems.
   */
  public class FileMetadataSourceConfig extends AbstractFileMetadataSourceConfig {

    @Description("Scheme of the source filesystem.")
    public String scheme;

    public FileMetadataSourceConfig(String name, String sourcePaths, Integer maxSplitSize, String scheme) {
      super(name, sourcePaths, maxSplitSize);
      this.scheme = scheme;
    }
  }
}
