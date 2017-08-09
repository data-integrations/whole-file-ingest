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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.batch.file.FileMetadata;
import org.apache.hadoop.fs.FileStatus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FTPFileMetadata extends FileMetadata {

  public static final String FTP_USERNAME = "ftpUsername";
  public static final String FTP_PASSWORD = "ftpPassword";
  public static final String SFTP_KEY_PATH = "sftpKeyPath";
  public static final Schema CREDENTIAL_SCHEMA = Schema.recordOf(
    "metadata",
    Schema.Field.of(FTP_USERNAME, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(FTP_PASSWORD, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of(SFTP_KEY_PATH, Schema.nullableOf(Schema.of(Schema.Type.STRING)))
  );

  private final String ftpUsername;
  private final String ftpPassword;
  private final String sftpKeyPath;

  public FTPFileMetadata(FileStatus fileStatus, String sourcePath, String ftpUsername, String ftpPassword,
                         String sftpKeyPath)
    throws IOException {
    super(fileStatus, sourcePath);
    this.ftpUsername = ftpUsername;
    this.ftpPassword = ftpPassword;
    this.sftpKeyPath = sftpKeyPath;
  }

  public FTPFileMetadata(StructuredRecord record) {
    super(record);
    this.ftpUsername = record.get(FTP_USERNAME);
    this.ftpPassword = record.get(FTP_PASSWORD);
    this.sftpKeyPath = record.get(SFTP_KEY_PATH);
  }

  public FTPFileMetadata(DataInput dataInput) throws IOException {
    super(dataInput);
    this.ftpUsername = dataInput.readUTF();
    this.ftpPassword = dataInput.readUTF();
    this.sftpKeyPath = dataInput.readUTF();
  }

  public String getFtpUsername() {
    return ftpUsername;
  }

  public String getFtpPassword() {
    return ftpPassword;
  }

  public String getSftpKeyPath() {
    return sftpKeyPath;
  }

  @Override
  protected Schema getCredentialSchema() {
    return CREDENTIAL_SCHEMA;
  }

  @Override
  protected void addCredentialsToRecordBuilder(StructuredRecord.Builder builder) {
    builder
      .set(FTP_USERNAME, ftpUsername)
      .set(FTP_PASSWORD, ftpPassword)
      .set(SFTP_KEY_PATH, sftpKeyPath);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    super.write(dataOutput);
    dataOutput.writeUTF(ftpUsername);
    dataOutput.writeUTF(ftpPassword);
    dataOutput.writeUTF(sftpKeyPath);
  }
}
