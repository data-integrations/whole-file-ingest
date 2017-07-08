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
package co.cask.hydrator.plugin;


import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Action to work with the Amazon S3 Client
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("AmazonS3Client")
@Description("Action to work with the Amazon S3 Client.")
public class AmazonS3ClientAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(AmazonS3ClientAction.class);

  private AmazonS3ClientActionConfig config;

  public AmazonS3ClientAction(AmazonS3ClientActionConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    config.validate();
  }

  @Override
  public void run(ActionContext context) throws Exception {
    config.validate();

    BasicAWSCredentials awsCredentials = new BasicAWSCredentials(config.accessKeyId, config.secretKeyId);
    AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
      .withRegion(config.region)
      .build();

    switch (config.action) {
      case "Create Bucket":
        Bucket newBucket = s3Client.createBucket(config.bucketName);
        context.getArguments().set(config.newBucketMacroLoc, newBucket.getName());
        break;
      case "Delete Bucket":
        s3Client.deleteBucket(config.bucketName);
        break;
      case "Copy Object":
        s3Client.copyObject(config.sourceBucketName, config.sourceObjectKey,
                            config.destBucketName, config.destObjectKey);
        break;
      default:
        throw new IllegalArgumentException(String.format("Action '%s' not implemented yet.", config.action));
    }

  }


  /**
   * The config for the Amazon S3 Client
   */
  public static class AmazonS3ClientActionConfig extends PluginConfig {
    @Macro
    @Description("Your AWS Access Key Id")
    private String action;

    @Macro
    @Description("Your AWS Access Key Id")
    private String accessKeyId;

    @Macro
    @Description("Your AWS Secret Key Id")
    private String secretKeyId;

    @Macro
    @Nullable
    @Description("The AWS Region to operate in")
    private String region;

    @Macro
    @Nullable
    @Description("The AWS Bucket Name to work with")
    private String bucketName;

    @Macro
    @Nullable
    @Description("The AWS Bucket Name to work with")
    private String newBucketMacroLoc;

    @Macro
    @Nullable
    @Description("The AWS Bucket Name to work with")
    private String sourceBucketName;

    @Macro
    @Nullable
    @Description("The AWS Bucket Name to work with")
    private String sourceObjectKey;

    @Macro
    @Nullable
    @Description("The AWS Bucket Name to work with")
    private String destBucketName;

    @Macro
    @Nullable
    @Description("The AWS Bucket Name to work with")
    private String destObjectKey;

    public AmazonS3ClientActionConfig(String action, String accessKeyId,
                                      String secretKeyId, String region,
                                      String bucketName) {
      this.action = action;
      this.accessKeyId = accessKeyId;
      this.secretKeyId = secretKeyId;
      this.region = region;
      this.bucketName = bucketName;
    }

    public AmazonS3ClientActionConfig(String action, String accessKeyId,
                                      String secretKeyId, String region,
                                      String bucketName, String newBucketMacroLoc) {
      this.action = action;
      this.accessKeyId = accessKeyId;
      this.secretKeyId = secretKeyId;
      this.region = region;
      this.bucketName = bucketName;
      this.newBucketMacroLoc = newBucketMacroLoc;
    }

    public AmazonS3ClientActionConfig(String action, String accessKeyId,
                                      String secretKeyId, String region,
                                      String sourceBucketName, String sourceObjectKey,
                                      String destBucketName, String destObjectKey) {
      this.action = action;
      this.accessKeyId = accessKeyId;
      this.secretKeyId = secretKeyId;
      this.region = region;
      this.sourceBucketName = sourceBucketName;
      this.sourceObjectKey = sourceObjectKey;
      this.destBucketName = destBucketName;
      this.destObjectKey = destObjectKey;
    }


    /**
     * Validates the config parameters required for unloading the data.
     */
    private void validate() throws IllegalArgumentException {
      // Check for required parameters
      // Check for required params for each action
    }
  }
}
