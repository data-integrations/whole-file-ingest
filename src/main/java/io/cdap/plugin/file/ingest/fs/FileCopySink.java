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

package io.cdap.plugin.file.ingest.fs;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.file.ingest.AbstractFileCopySink;
import io.cdap.plugin.file.ingest.AbstractFileCopySinkConfig;
import io.cdap.plugin.file.ingest.FileCopyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * FileCopySink that writes to local filesystem or local HDFS.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("FileCopySink")
@Description("Copies files from remote filesystem to local filesystem or local HDFS.")
public class FileCopySink extends AbstractFileCopySink {

  private FileCopySinkConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(FileCopySink.class);

  public FileCopySink(FileCopySinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    super.prepareRun(context);
    context.addOutput(Output.of(config.referenceName, new FileCopyOutputFormatProvider(config)));
  }

  /**
   * Adds necessary configuration resources and provides OutputFormat Class
   */
  public class FileCopyOutputFormatProvider extends AbstractFileCopySink.FileCopyOutputFormatProvider {
    public FileCopyOutputFormatProvider(AbstractFileCopySinkConfig config) {
      super(config);
      switch (config.getScheme()) {
        case "file" :
          try {
            conf.put(FileCopyOutputFormat.FS_HOST_URI,
                     new URI(config.getScheme(), null, Path.SEPARATOR, null).toString());
          } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
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
