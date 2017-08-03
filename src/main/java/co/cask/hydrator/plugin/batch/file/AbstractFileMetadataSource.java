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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.plugin.common.ReferenceBatchSource;
import co.cask.hydrator.plugin.common.ReferencePluginConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

/**
 * Abstract class for FileCopySource plugin. Extracts metadata of desired files
 * from the source database.
 * @param <K> the FileMetadata class specific to each filesystem.
 */
public abstract class AbstractFileMetadataSource<K extends FileMetadata>
  extends ReferenceBatchSource<NullWritable, K, StructuredRecord> {

  private final AbstractFileMetadataSourceConfig config;

  protected AbstractFileMetadataSource(AbstractFileMetadataSourceConfig config) {
    super(config);
    this.config = config;
  }

  /**
   * Loads configurations from UI and check if they are valid.
   */
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    this.config.validate();
  }

  /**
   * Initialize the output StructuredRecord Schema here.
   * @param context
   * @throws Exception
   */
  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  /**
   * Load job configurations here.
   */
  public void prepareRun(BatchSourceContext context) throws Exception {
    config.validate();
  }

  /**
   * Convert file metadata to StructuredRecord and emit.
   */
  public abstract void transform(KeyValue<NullWritable, K> input, Emitter<StructuredRecord> emitter);

  /**
   * Abstract class for the configuration of FileCopySink
   */
  public abstract class AbstractFileMetadataSourceConfig extends ReferencePluginConfig {

    @Macro
    @Description("Collection of sourcePaths separated by \",\" to read files from")
    public String sourcePaths;

    @Macro
    @Description("The number of files each split reads in")
    public Integer maxSplitSize;

    @Description("Whether or not to copy recursively")
    public Boolean recursiveCopy;

    public AbstractFileMetadataSourceConfig(String name, String sourcePaths,
                                            Integer maxSplitSize) {
      super(name);
      this.sourcePaths = sourcePaths;
      this.maxSplitSize = maxSplitSize;
    }

    public void validate() {
      if (!this.containsMacro("maxSplitSize")) {
        if (maxSplitSize <= 0) {
          throw new IllegalArgumentException("Max split size must be a positive integer.");
        }
      }
    }
  }

  /**
   * This method initializes the configuration instance with fields that are shared by all plugins.
   *
   * @param conf The configuration we wish to initialize.
   */
  protected void setDefaultConf(Configuration conf) {
    MetadataInputFormat.setSourcePaths(conf, config.sourcePaths);
    MetadataInputFormat.setMaxSplitSize(conf, config.maxSplitSize);
    MetadataInputFormat.setRecursiveCopy(conf, config.recursiveCopy.toString());
  }

    /*
     * Put additional configurations here for specific databases.
     */
}
