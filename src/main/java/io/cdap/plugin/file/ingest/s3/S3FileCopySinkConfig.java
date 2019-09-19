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

package io.cdap.plugin.file.ingest.s3;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.file.ingest.AbstractFileCopySinkConfig;

import java.net.URI;
import java.util.Set;

/**
 * Additional configurations for connecting to an S3 filesystem.
 */
public class S3FileCopySinkConfig extends AbstractFileCopySinkConfig {
  public static final String SCHEME = "scheme";
  private static final Set<String> ALLOWED_SCHEME_TYPES = ImmutableSet.of("s3a", "s3n");

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

  private S3FileCopySinkConfig(Builder builder) {
    super(builder.referenceName, builder.basePath, builder.enableOverwrite, builder.preserveFileOwner,
          builder.bufferSize);
    filesystemURI = builder.filesystemURI;
    accessKeyId = builder.accessKeyId;
    secretKeyId = builder.secretKeyId;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(S3FileCopySinkConfig copy) {
    return new S3FileCopySinkConfig.Builder()
      .setReferenceName(copy.referenceName)
      .setBasePath(copy.basePath)
      .setEnableOverwrite(copy.enableOverwrite)
      .setPreserveFileOwner(copy.preserveFileOwner)
      .setBufferSize(copy.bufferSize)
      .setFilesystemURI(copy.filesystemURI)
      .setAccessKeyId(copy.accessKeyId)
      .setSecretKeyId(copy.secretKeyId);
  }

  @Override
  public String getScheme() {
    return URI.create(filesystemURI).getScheme();
  }

  @Override
  public void validate(FailureCollector failureCollector) {
    super.validate(failureCollector);
    if (!ALLOWED_SCHEME_TYPES.contains(getScheme())) {
      failureCollector.addFailure("Scheme must be either s3a or s3n.", null)
        .withConfigProperty(SCHEME);
    }
  }

  /**
   * Builder for creating a {@link S3FileCopySinkConfig}.
   */
  public static final class Builder {
    private String referenceName;
    private String basePath;
    private Boolean enableOverwrite;
    private Boolean preserveFileOwner;
    private Integer bufferSize;
    private String filesystemURI;
    private String accessKeyId;
    private String secretKeyId;

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

    public Builder setFilesystemURI(String val) {
      filesystemURI = val;
      return this;
    }

    public Builder setAccessKeyId(String val) {
      accessKeyId = val;
      return this;
    }

    public Builder setSecretKeyId(String val) {
      secretKeyId = val;
      return this;
    }

    public S3FileCopySinkConfig build() {
      return new S3FileCopySinkConfig(this);
    }
  }
}
