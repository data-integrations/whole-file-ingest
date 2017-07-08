# Amazon S3 Client Action


Description
-----------
The Amazon S3 Client Action is used to work with S3 buckets and objects before or after the execution of a pipeline.

Use Case
--------
As a user, I need to copy files from a client's S3 bucket to my own S3 bucket in order to process it. I can use this action to do that.

Properties
----------
| Configuration | Required | Default | Description |
| :------------ | :------: | :------ | :---------- |
| **Action** | **Y** | None | The action the plugin should perform. Based on this, different configuration options may be required. |
| **Amazon Access Key Id** | **Y** | None | The Amazon Access Key that has permission to perform the specified Action. |
| **Amazon Secret Key Id** | **Y** | None | The Amazon Secret Key that has permission to perform the specified Action. |
| **Region** | **Y** | None | The Amazon Region where the S3 action is to be performed. |
| **Source Bucket Name** | **For Copy Object Action** | None | The bucket where the file currently exists that you want to copy. |
| **Source Object Key** | **For Copy Object Action** | None | The object key to copy. If the file is in a folder, this includes the full path to the object. |
| **Destination Bucket Name** | **For Copy Object Action** | None | The bucket to copy the file to. |
| **Destination Object Key** | **For Copy Object Action** | None | The new object key of the file. If the file is in a folder, this is the full path to the new object. |
| **Bucket Name** | **For Create or Delete Bucket Action** | None | The name of the bucket to create or delete. |
| **New Bucket Macro Location** | **For Create Bucket Action** | None | The name of the macro that is to contain the new bucket name. This can be used in your pipeline if the name of the bucket is dynamic. |

Usage Notes
-----------

TODO