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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * FileCopySource plugin that pulls filemetadata from FTP Filesystem.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("FTPFileMetadataSource")
@Description("Reads file metadata from FTP bucket.")
public class FTPFileMetadataSource extends AbstractFileMetadataSource<FTPFileMetadata> {

  private FTPFileMetadataSourceConfig config;

  public FTPFileMetadataSource(FTPFileMetadataSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    List<Schema.Field> fieldList = new ArrayList<>(FileMetadata.DEFAULT_SCHEMA.getFields());
    fieldList.addAll(FTPFileMetadata.CREDENTIAL_SCHEMA.getFields());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.recordOf("FTPSchema", fieldList));
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    super.prepareRun(context);
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    //initialize configuration
    setDefaultConf(conf);
    FTPMetadataInputFormat.setURI(conf, config.filesystemURI);
    URI hostURI = URI.create(config.filesystemURI);
    switch (hostURI.getScheme()) {
      case "ftp":
        FTPMetadataInputFormat.setFTPUsername(conf, hostURI.getHost(), config.username);
        FTPMetadataInputFormat.setFTPHost(conf, hostURI.getHost());
        FTPMetadataInputFormat.setFTPFsClass(conf);
        if (config.password != null) {
          FTPMetadataInputFormat.setFTPassword(conf, hostURI.getHost(), config.password);
        }
        break;
      case "sftp":
        FTPMetadataInputFormat.setSFTPUsername(conf, hostURI.getHost(), config.username);
        FTPMetadataInputFormat.setSFTPHost(conf, hostURI.getHost());
        FTPMetadataInputFormat.setSFTPFsClass(conf);
        if (config.password != null) {
          FTPMetadataInputFormat.setSFTPPassword(conf, hostURI.getHost(), config.password);
        }
        if (config.keyPath != null) {
          FTPMetadataInputFormat.setSFTPKeyFilePath(conf, config.keyPath);
        }
        break;
      default:
        throw new IllegalArgumentException("Scheme must be either ftp or sftp");
    }

    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(FTPMetadataInputFormat.class, conf)));
  }

  @Override
  public void transform(KeyValue<NullWritable, FTPFileMetadata> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue().toRecord());
  }

  /**
   * Configurations required for connecting to FTPFilesystem.
   */
  public class FTPFileMetadataSourceConfig extends AbstractFileMetadataSourceConfig {

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
    @Description("Path to ssh keyfile")
    public String keyPath;

    public FTPFileMetadataSourceConfig(String name, String sourcePaths, Integer maxSplitSize,
                                       String filesystemURI, String username, @Nullable String password,
                                       @Nullable String keyPath) {
      super(name, sourcePaths, maxSplitSize);
      this.filesystemURI = filesystemURI;
      this.username = username;
      this.password = password;
      this.keyPath = keyPath;
    }

    @Override
    public void validate() {
      super.validate();
      if (!this.containsMacro("filesystemURI")) {
        URI fsUri = URI.create(filesystemURI);
        if (!fsUri.getScheme().equals("ftp") && !fsUri.getScheme().equals("sftp")) {
          throw new IllegalArgumentException("URI scheme for FTP source must be ftp or sftp");
        }
      }
    }
  }
}
