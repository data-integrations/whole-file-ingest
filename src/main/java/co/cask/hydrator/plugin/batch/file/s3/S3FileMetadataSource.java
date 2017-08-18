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

package co.cask.hydrator.plugin.batch.file.s3;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
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
import co.cask.hydrator.plugin.common.JobUtils;
import co.cask.hydrator.plugin.common.SourceInputFormatProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * FileCopySource plugin that pulls filemetadata from S3 Filesystem.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("S3FileMetadataSource")
@Description("Reads file metadata from S3 bucket.")
public class S3FileMetadataSource extends AbstractFileMetadataSource<S3FileMetadata> {
  private S3FileMetadataSourceConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(S3FileMetadataSource.class);

  public S3FileMetadataSource(S3FileMetadataSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    List<Schema.Field> fieldList = new ArrayList<>(FileMetadata.DEFAULT_SCHEMA.getFields());
    fieldList.addAll(S3FileMetadata.CREDENTIAL_SCHEMA.getFields());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.recordOf("S3Schema", fieldList));
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    super.prepareRun(context);
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    // initialize configuration
    setDefaultConf(conf);
    S3MetadataInputFormat.setURI(conf, config.filesystemURI);
    String fsScheme = URI.create(config.filesystemURI).getScheme();
    switch (fsScheme) {
      case "s3a" :
        S3MetadataInputFormat.setS3aAccessKeyId(conf, config.accessKeyId);
        S3MetadataInputFormat.setS3aSecretKeyId(conf, config.secretKeyId);
        S3MetadataInputFormat.setS3aFsClass(conf);
        break;
      case "s3n" :
        S3MetadataInputFormat.setS3nAccessKeyId(conf, config.accessKeyId);
        S3MetadataInputFormat.setS3nSecretKeyId(conf, config.secretKeyId);
        S3MetadataInputFormat.setS3nFsClass(conf);
        break;
      default:
        throw new IllegalArgumentException("Scheme must be either s3a or s3n.");
    }

    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(S3MetadataInputFormat.class, conf)));
  }

  @Override
  public void transform(KeyValue<NullWritable, S3FileMetadata> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue().toRecord());
  }

  /**
   * Configurations required for connecting to S3Filesystem.
   */
  public class S3FileMetadataSourceConfig extends AbstractFileMetadataSourceConfig {

    // configurations for S3
    @Macro
    @Description("The URI of the filesystem")
    public String filesystemURI;

    @Macro
    @Description("Your AWS Access Key Id")
    public String accessKeyId;

    @Macro
    @Description("Your AWS Secret Key Id")
    public String secretKeyId;

    public S3FileMetadataSourceConfig(String name, String sourcePaths, Integer maxSplitSize,
                                      String filesystemURI, String accessKeyId,
                                      String secretKeyId) {
      super(name, sourcePaths, maxSplitSize);
      this.filesystemURI = filesystemURI;
      this.accessKeyId = accessKeyId;
      this.secretKeyId = secretKeyId;
    }

    @Override
    public void validate() {
      super.validate();
      if (!this.containsMacro("filesystemURI")) {
        URI fsUri = URI.create(filesystemURI);
        if (!fsUri.getScheme().equals("s3a") && !fsUri.getScheme().equals("s3n")) {
          throw new IllegalArgumentException("URI scheme for S3 source must be s3a or s3n");
        }
      }
    }
  }
}
