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

package co.cask.hydrator.plugin.batch.file.hdfs;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.plugin.batch.file.AbstractFileCopySink;
import co.cask.hydrator.plugin.batch.file.FileCopyOutputFormat;
import com.sun.istack.Nullable;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * FileCopySink that writes to local filesystem or local HDFS.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("LocalFileCopySink")
@Description("Copies files from remote filesystem to local filesystem or local HDFS.")
public class LocalFileCopySink extends AbstractFileCopySink {

  private LocalFileCopySinkConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(LocalFileCopySink.class);

  public LocalFileCopySink(LocalFileCopySinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    config.validate();
    context.addOutput(Output.of(config.referenceName, new LocalFileCopyOutputFormatProvider(config)));
  }

  /**
   * Configurations required for connecting to HDFS.
   */
  public class LocalFileCopySinkConfig extends AbstractFileCopySinkConfig {

    @Description("Scheme of the destination filesystem.")
    public String localScheme;

    public LocalFileCopySinkConfig(String name, String basePath, Boolean enableOverwrite,
                                   Boolean preserveFileOwner, @Nullable Integer bufferSize, String localScheme) {
      super(name, basePath, enableOverwrite, preserveFileOwner, bufferSize);
      this.localScheme = localScheme;
    }

    @Override
    public String getScheme() {
      return localScheme;
    }
  }

  /**
   * Adds necessary configuration resources and provides OutputFormat Class
   */
  public class LocalFileCopyOutputFormatProvider extends FileCopyOutputFormatProvider {
    public LocalFileCopyOutputFormatProvider(AbstractFileCopySinkConfig config) {
      super(config);
      switch (config.getScheme()) {
        case "file" :
          try {
            conf.put(FileCopyOutputFormat.FS_HOST_URI,
                     new URI(config.getScheme(), null, Path.SEPARATOR, null).toString());
          } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage());
          }
          break;
        case "hdfs" :
          break;
        default:
          throw new IllegalArgumentException("Scheme must be either file or hdfs.");
      }
    }
  }
}
