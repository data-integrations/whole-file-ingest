package co.cask.hydrator.plugin.batch.file;

import co.cask.hydrator.plugin.batch.file.ftp.FTPMetadataInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

import java.net.URI;

public class SFTPFilesystemTest {
  @Test
  public void FTPtest() throws Exception {
    Configuration conf = new Configuration(false);
    conf.clear();
    final String host = "filecopycluster25455-1000.dev.continuuity.net";

    FTPMetadataInputFormat.setSFTPHost(conf, host);
    FTPMetadataInputFormat.setSFTPUsername(conf, host, "chester");
    //FTPMetadataInputFormat.setSFTPPassword(conf, host, "Realtime123!");
    FTPMetadataInputFormat.setSFTPKeyFilePath(conf, "/Users/chestercheng/.ssh/id_rsa");
    FTPMetadataInputFormat.setSFTPFsClass(conf);
    FTPMetadataInputFormat.setURI(conf, new URI("sftp", host, null, null).toString());
    FTPMetadataInputFormat.setMaxSplitSize(conf, 128);
    FTPMetadataInputFormat.setRecursiveCopy(conf, String.valueOf(true));
    FTPMetadataInputFormat.setSourcePaths(conf, "/home/chester/");

    TaskAttemptContextImpl context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    FTPMetadataInputFormat metadataInputFormat = new FTPMetadataInputFormat();
    metadataInputFormat.getSplits(context);
  }
}
