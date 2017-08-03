# Local File Metadata Batch Source

Description
-----------
The Local File Metadata plugin is a source plugin that allows users to read file metadata from a local HDFS or a local filesystem.


Use Case
--------
Use this source to extract the metadata of files under specified paths. The metadata can be passed to the
S3FileCopySink and the files will be copied from a local filesystem to an Amazon S3 Filesystem.


Properties
----------
| Configuration          | Required | Default   | Description                                                                                                                                                                                                                            |
| :--------------------- | :------: | :------   | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Reference Name**     |  **Y**   | None      | This will be used to uniquely identify this source for lineage, annotating metadata, etc.                                                                                                                                              |
| **Scheme**             |  **Y**   | file      | The scheme of the local destination filesystem. Use "file" for writing to the local filesystem and "hdfs" for local HDFS.                                                                                                              |
| **Source Paths**       |  **Y**   | None      | Path(s) to file(s) to be read. If a directory is specified, end the path name with a '/'.                                                                                                                                              |
| **Max Split Size**     |  **Y**   | None      | Specifies the number of files that are controlled by each split. The number of splits created will be the total number of files divided by Max Split Size. The InputFormat will assign roughly the same number of bytes to each split. |
| **Copy Recursively**   |  **Y**   | True      | Whether or not to copy recursively. Similar to the `-r` option in the `cp` terminal command. Set this to true if you want to copy the entire directory recursively.                                                                    |

Usage Notes
-----------
This source plugin only reads filemetadata from a local source filesystem. To copy files, pass its outputs to a FileCopySink of the destination Filesystem.
A StructuredRecord with the following schema is emitted for each file it reads.

| Field                  | Type   | Description                                                                                                                                    |
| :--------------------- | :----- | :-------------------------                                                                                                                     |
| **fileName**           | String | Only contains the name of the file.                                                                                                            |
| **fullPath**           | String | Contains the full path of the file in the source file system.                                                                                  |
| **fileSize**           | long   | File Szie in bytes.                                                                                                                            |
| **hostURI**            | String | The source filesystem's URI.                                                                                                                   |
| **modificationTime**   | long   | The modification timestamp of the file.                                                                                                        |
| **group**              | String | The group that the file belongs to.                                                                                                            |
| **owner**              | String | The owner of the file.                                                                                                                         |
| **isFolder**           | Boolean| Whether or not the file is a folder.                                                                                                           |
| **relativePath**       | String | The relative path is constructed by deleting the portion of the source path that comes before the last path separator ("/") from the full path.|
| **permission**         | int    | The file's access permission                                                                                                                   |

