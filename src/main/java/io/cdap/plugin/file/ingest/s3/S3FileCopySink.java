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

package io.cdap.plugin.file.ingest.s3;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.file.ingest.AbstractFileCopySink;
import io.cdap.plugin.file.ingest.FileCopyOutputFormat;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;

import java.net.URI;

/**
 * FileCopySink that writes to S3.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("S3FileCopySink")
@Description("Copies files from remote filesystem to S3 Filesystem")
public class S3FileCopySink extends AbstractFileCopySink {

  private S3FileCopySinkConfig config;

  public S3FileCopySink(S3FileCopySinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    config.validate();
    context.addOutput(Output.of(config.referenceName, new S3FileCopyOutputFormatProvider(config)));
  }

  /**
   * Additional configurations for connecting to an S3 filesystem.
   */
  public class S3FileCopySinkConfig extends AbstractFileCopySinkConfig {

    // configurations for S3
    @Macro
    @Description("The URI of the destination filesystem")
    public String filesystemURI;

    @Macro
    @Description("Your AWS Access Key Id")
    public String accessKeyId;

    @Macro
    @Description("Your AWS Secret Key Id")
    public String secretKeyId;

    public S3FileCopySinkConfig(String name, String basePath, Boolean enableOverwrite,
                                Boolean preserveFileOwner, Integer bufferSize, String filesystemURI,
                                String accessKeyId, String secretKeyId) {
      super(name, basePath, enableOverwrite, preserveFileOwner, bufferSize);
      this.filesystemURI = filesystemURI;
      this.accessKeyId = accessKeyId;
      this.secretKeyId = secretKeyId;
    }

    @Override
    public String getScheme() {
      return URI.create(filesystemURI).getScheme();
    }
  }

  /**
   * Adds necessary configuration resources and provides OutputFormat Class
   */
  public class S3FileCopyOutputFormatProvider extends FileCopyOutputFormatProvider {
    public S3FileCopyOutputFormatProvider(AbstractFileCopySinkConfig config) {
      super(config);
      S3FileCopySinkConfig s3Config = (S3FileCopySinkConfig) config;

      FileCopyOutputFormat.setFilesystemHostUri(conf, s3Config.filesystemURI);

      switch (config.getScheme()) {
        case "s3a" :
          conf.put(S3MetadataInputFormat.S3A_ACCESS_KEY_ID, s3Config.accessKeyId);
          conf.put(S3MetadataInputFormat.S3A_SECRET_KEY_ID, s3Config.secretKeyId);
          conf.put(S3MetadataInputFormat.S3A_FS_CLASS, S3AFileSystem.class.getName());
          break;
        case "s3n" :
          conf.put(S3MetadataInputFormat.S3N_ACCESS_KEY_ID, s3Config.accessKeyId);
          conf.put(S3MetadataInputFormat.S3N_SECRET_KEY_ID, s3Config.secretKeyId);
          conf.put(S3MetadataInputFormat.S3N_FS_CLASS, NativeS3FileSystem.class.getName());
          break;
        default:
          throw new IllegalArgumentException("Scheme must be either s3a or s3n.");
      }

    }
  }
}
