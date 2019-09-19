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

package io.cdap.plugin.file.ingest.fs;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.file.ingest.AbstractFileCopySinkConfig;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * Configurations required for connecting to HDFS.
 */
public class FileCopySinkConfig extends AbstractFileCopySinkConfig {
  public static final String SCHEME = "scheme";
  private static final Set<String> ALLOWED_SCHEME_TYPES = ImmutableSet.of("file", "hdfs");

  @Description("Scheme of the destination filesystem.")
  public String scheme;

  public FileCopySinkConfig(String name, String basePath, Boolean enableOverwrite,
                            Boolean preserveFileOwner, @Nullable Integer bufferSize, String scheme) {
    super(name, basePath, enableOverwrite, preserveFileOwner, bufferSize);
    this.scheme = scheme;
  }

  private FileCopySinkConfig(Builder builder) {
    super(builder.referenceName, builder.basePath, builder.enableOverwrite, builder.preserveFileOwner,
          builder.bufferSize);
    scheme = builder.scheme;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(FileCopySinkConfig copy) {
    return new Builder()
      .setReferenceName(copy.referenceName)
      .setBasePath(copy.basePath)
      .setEnableOverwrite(copy.enableOverwrite)
      .setPreserveFileOwner(copy.preserveFileOwner)
      .setBufferSize(copy.bufferSize)
      .setScheme(copy.scheme);
  }

  @Override
  public String getScheme() {
    return scheme;
  }

  @Override
  public void validate(FailureCollector failureCollector) {
    super.validate(failureCollector);
    if (!ALLOWED_SCHEME_TYPES.contains(scheme)) {
      failureCollector.addFailure("Scheme must be either file or hdfs.", null)
        .withConfigProperty(SCHEME);
    }
  }

  /**
   * Builder for creating a {@link FileCopySinkConfig}.
   */
  public static final class Builder {
    private String referenceName;
    private String basePath;
    private Boolean enableOverwrite;
    private Boolean preserveFileOwner;
    private Integer bufferSize;
    private String scheme;

    private Builder() {
    }

    public Builder setReferenceName(String val) {
      referenceName = val;
      return this;
    }

    public Builder setBasePath(String val) {
      basePath = val;
      return this;
    }

    public Builder setEnableOverwrite(Boolean val) {
      enableOverwrite = val;
      return this;
    }

    public Builder setPreserveFileOwner(Boolean val) {
      preserveFileOwner = val;
      return this;
    }

    public Builder setBufferSize(Integer val) {
      bufferSize = val;
      return this;
    }

    public Builder setScheme(String val) {
      scheme = val;
      return this;
    }

    public FileCopySinkConfig build() {
      return new FileCopySinkConfig(this);
    }
  }
}
