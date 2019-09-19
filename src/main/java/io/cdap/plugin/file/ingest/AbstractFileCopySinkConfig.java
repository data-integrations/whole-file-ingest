/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.file.ingest;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;

import javax.annotation.Nullable;

/**
 * Abstract class for the configuration of FileCopySink
 */
public abstract class AbstractFileCopySinkConfig extends ReferencePluginConfig {
  public static final String BUFFER_SIZE = "bufferSize";

  @Macro
  @Description("The destination path. Will be created if it doesn't exist.")
  public String basePath;

  @Description("Whether or not to overwrite if the file already exists.")
  public Boolean enableOverwrite;

  @Description("Whether or not to preserve the owner of the file from source filesystem.")
  public Boolean preserveFileOwner;

  @Macro
  @Nullable
  @Description("The size of the buffer (in MB) that temporarily stores data from file input stream. Defaults to" +
    " 1 MB")
  public Integer bufferSize;

  public AbstractFileCopySinkConfig(String name, String basePath, Boolean enableOverwrite,
                                    Boolean preserveFileOwner, @Nullable Integer bufferSize) {
    super(name);
    this.basePath = basePath;
    this.enableOverwrite = enableOverwrite;
    this.preserveFileOwner = preserveFileOwner;
    this.bufferSize = bufferSize;
  }

  public void validate(FailureCollector failureCollector) {
    IdUtils.validateReferenceName(referenceName, failureCollector);

    if (!this.containsMacro(BUFFER_SIZE)) {
      // check if it's null since bufferSize isn't a required field
      if (bufferSize != null && bufferSize <= 0) {
        failureCollector.addFailure("Buffer size must be a positive integer.", null)
          .withConfigProperty(BUFFER_SIZE);
      }
    }
  }

  /**
   * Gets schema of filesystem.
   *
   * @return schema.
   */
  public abstract String getScheme();
}
