# File Copy Batch Sink

Description
-----------
The File Copy plugin is a sink plugin that takes file metadata records as inputs and copies the files into the local HDFS or the local filesystem.


Use Case
--------
Use this sink to copy files from any source to your local HDFS or filesystem.
You may want to periodically sync your HDFS with some remote filesystem. Schedule a pipeline with this plugin to copy new files periodically.


Properties
----------
| Configuration                            | Required | Default   | Description                                                                                                                  |
| :--------------------------------------- | :------: | :------   | :--------------------------------------------------------------------------------------------------------------------------- |
| **Reference Name**                       |  **Y**   | None      | This will be used to uniquely identify this source for lineage, annotating metadata, etc.                                    |
| **Scheme**                               |  **Y**   | file      | The scheme of the local destination filesystem. Use "file" for writing to the local filesystem and "hdfs" for local HDFS.    |
| **Base Path**                            |  **Y**   | None      | The folder where the copied files will be placed. It will be created if it doesn't exist.                                    |
| **Enable Overwrite**                     |  **Y**   | False     | Specifies whether or not to overwrite files if it already exists.                                                            |
| **Preserve File Owner**                  |  **Y**   | False     | Whether or not to preserve the owner of the file from source filesystem.                                                     |
| **Buffer Size**                          |  **N**   | 1 MB      | The size of the buffer (in Megabytes) that temporarily stores data from file input stream while copying. Defaults to 1 MB.   |

Usage Notes
-----------
This sink plugin only reads StructuredRecords with the following schema. Each record should contain the metadata for a file to be copied.

| Field                  | Type   | Description                                                                                                                                    |
| :--------------------- | :----- | :-------------------------                                                                                                                     |
| **fileName**           | String | Only contains the name of the file.                                                                                                            |
| **fullPath**           | String | Contains the full path of the file in the source file system.                                                                                  |
| **fileSize**           | long   | File size in bytes.                                                                                                                            |
| **hostURI**            | String | The source filesystem's URI.                                                                                                                   |
| **modificationTime**   | long   | The modification timestamp of the file.                                                                                                        |
| **group**              | String | The group that the of the file belongs to.                                                                                                     |
| **owner**              | String | The owner of the file.                                                                                                                         |
| **isDir**              | Boolean| Whether or not the file is a directory.                                                                                                        |
| **relativePath**       | String | The relative path is constructed by deleting the portion of the source path that comes before the last path separator ("/") from the full path.|
| **permission**         | int    | The file's access permission                                                                                                                   |
| **Credentials**        | Record | Additional information required to connect to the source Filesystem.                                                                           |
