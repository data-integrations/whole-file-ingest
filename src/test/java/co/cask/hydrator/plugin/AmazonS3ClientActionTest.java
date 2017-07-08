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

import co.cask.cdap.etl.mock.action.MockActionContext;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import com.amazonaws.regions.Regions;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link AmazonS3ClientAction}
 */
@Ignore
public class AmazonS3ClientActionTest {
  private static String AWS_ACCESS_KEY_ID = "";
  private static String AWS_SECRET_KEY_ID = "";

  @Test
  public void testCreateBucket() throws Exception {
    AmazonS3ClientAction.AmazonS3ClientActionConfig config =
      new AmazonS3ClientAction.AmazonS3ClientActionConfig("Create Bucket",
                                                          AWS_ACCESS_KEY_ID,
                                                          AWS_SECRET_KEY_ID,
                                                          Regions.US_WEST_1.getName(),
                                                          "cask-test-bucket-" + System.currentTimeMillis(),
                                                          "s3-bucket");
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new AmazonS3ClientAction(config).configurePipeline(configurer);
    new AmazonS3ClientAction(config).run(new MockActionContext());
  }

  @Test
  public void testDeleteBucket() throws Exception {
    AmazonS3ClientAction.AmazonS3ClientActionConfig config =
      new AmazonS3ClientAction.AmazonS3ClientActionConfig("Delete Bucket",
                                                          AWS_ACCESS_KEY_ID,
                                                          AWS_SECRET_KEY_ID,
                                                          Regions.US_WEST_1.getName(),
                                                          "cask-test-bucket-1496978497453");
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new AmazonS3ClientAction(config).configurePipeline(configurer);
    new AmazonS3ClientAction(config).run(new MockActionContext());
  }

  @Test
  public void testCopyObject() throws Exception {
    AmazonS3ClientAction.AmazonS3ClientActionConfig config =
      new AmazonS3ClientAction.AmazonS3ClientActionConfig("Copy Object",
                                                          AWS_ACCESS_KEY_ID,
                                                          AWS_SECRET_KEY_ID,
                                                          Regions.US_WEST_1.getName(),
                                                          "cask-test-bucket",
                                                          "some/path/to/a/file.txt",
                                                          "cask-test-bucket-1496979157026",
                                                          "some/path/to/a/file.txt");
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    new AmazonS3ClientAction(config).configurePipeline(configurer);
    new AmazonS3ClientAction(config).run(new MockActionContext());
  }
}
