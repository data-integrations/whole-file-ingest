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


Build
-----
To build your plugins:

    mvn clean package -DskipTests

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

UI Integration
--------------
The CDAP UI displays each plugin property as a simple textbox. To customize how the plugin properties
are displayed in the UI, you can place a configuration file in the ``widgets`` directory.
The file must be named following a convention of ``[plugin-name]-[plugin-type].json``.

See [Plugin Widget Configuration](http://docs.cdap.io/cdap/current/en/hydrator-manual/developing-plugins/packaging-plugins.html#plugin-widget-json)
for details on the configuration file.

The UI will also display a reference doc for your plugin if you place a file in the ``docs`` directory
that follows the convention of ``[plugin-name]-[plugin-type].md``.

When the build runs, it will scan the ``widgets`` and ``docs`` directories in order to build an appropriately
formatted .json file under the ``target`` directory. This file is deployed along with your .jar file to add your
plugins to CDAP.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/plugin.jar> config-file <target/plugin.json>

For example, here if your artifact is named 'azure-decompress-action-1.0.0.jar':

    > load artifact target/azure-decompress-action-1.0.0.jar config-file target/azure-decompress-action-1.0.0.json

## Mailing Lists

CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## IRC Channel

CDAP IRC Channel: #cdap on irc.freenode.net


## License and Trademarks

Copyright © 2016-2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
