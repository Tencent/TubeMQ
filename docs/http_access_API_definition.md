# HTTP access API definition

## Master metadata configuration API

### `admin_online_broker_configure`

Online the configuration of the Brokers which are new or offline. The Topics' configuration will be distributed to relate Brokers as well.

__Request__

|name|must|description|type|
|---|---|---|---|
|brokerId|yes|the id of broker. It supports bulk brokerId which separated by `,`. The maximum <br> number of a bulk is 50. The brokerId should be distinct in case of bulk value  |int|
|modifyUser| yes|the user who executes this |String|
|modifyDate| no|the modify date in the format of "yyyyMMddHHmmss"|String|
|confModAuthToken| yes|the authorization key |String|

__Response__

|name|description|type|
|---|---|---|
|code| return 0 if success, otherwise failed | int|
|errMsg| "OK" if success, other return error message| string|

### `admin_reload_broker_configure`

Update the configuration of the Brokers which are __online__. The new configuration will be published to Broker server, it
 will return error if the broker is offline.

__Request__

|name|must|description|type|
|---|---|---|---|
|brokerId|yes|the id of broker. It supports bulk brokerId which separated by `,`. The maximum <br> number of a bulk is 50. The brokerId should be distinct in case of bulk value  |int|
|modifyUser| yes|the user who executes this |String|
|modifyDate| no|the modify date in the format of "yyyyMMddHHmmss"|String|
|confModAuthToken| yes|the authorization key |String|

__Response__

|name|description|type|
|---|---|---|
|code| return 0 if success, otherwise failed | int|
|errMsg| "OK" if success, other return error message| string|

### `admin_offline_broker_configure`

Offline the configuration of the Brokers which are __online__. It should be called before Broker offline or retired.
The Broker processes can be terminated once all offline tasks are done.

__Request__

|name|must|description|type|
|---|---|---|---|
|brokerId|yes|the id of broker. It supports bulk brokerId which separated by `,`. The maximum <br> number of a bulk is 50. The brokerId should be distinct in case of bulk value  |int|
|modifyUser| yes|the user who executes this |String|
|modifyDate| no|the modify date in the format of "yyyyMMddHHmmss"|String|
|confModAuthToken| yes|the authorization key |String|

__Response__

|name|description|type|
|---|---|---|
|code| return 0 if success, otherwise failed | int|
|errMsg| "OK" if success, other return error message| string|

### `admin_set_broker_read_or_write`

Set Broker into a read-only or write-only state. Only Brokers are online and idle can be handled.

__Request__

|name|must|description|type|
|---|---|---|---|
|brokerId|yes|the id of broker. It supports bulk brokerId which separated by `,`. The maximum <br> number of a bulk is 50. The brokerId should be distinct in case of bulk value  |int|
|isAcceptPublish| yes|whether the brokers accept publish requests, default is true |Boolean|
|isAcceptSubscribe| no|whether the brokers accept subscribe requests, default is true|Boolean|
|modifyUser| yes|the user who request the change, default is creator |String|
|modifyDate| no|the modify date in the format of "yyyyMMddHHmmss"|String|
|confModAuthToken| yes|the authorization key |String|

__Response__

|name|description|type|
|---|---|---|
|code| return 0 if success, otherwise failed | int|
|errMsg| "OK" if success, other return error message| string|

### `admin_query_broker_run_status`

Query Broker status. Only the Brokers processes are __offline__ and idle can be terminated.

__Request__

|name|must|description|type|
|---|---|---|---|
|brokerId|yes|the id of broker. It supports bulk brokerId which separated by `,`. The maximum <br> number of a bulk is 50. The brokerId should be distinct in case of bulk value  |int|
|onlyAutoForbidden| yes|only auto forbidden set, default is false |Boolean|
|onlyEnableTLS| no|only enable TLS set, default is false|Boolean|
|withDetail| yes|whether it needs detail, default is false |Boolean|

__Response__

|name|description|type|
|---|---|---|
|code| return 0 if success, otherwise failed | int|
|errMsg| "OK" if success, other return error message| string|

### `admin_release_broker_autoforbidden_status`

Release the brokers' auto forbidden status.

__Request__

|name|must|description|type|
|---|---|---|---|
|brokerId|yes|the id of broker. It supports bulk brokerId which separated by `,`. The maximum <br> number of a bulk is 50. The brokerId should be distinct in case of bulk value  |int|
|realReason| yes|the reason of why it needs to release|String|
|modifyUser| yes|the user who request the change, default is creator |String|
|modifyDate| no|the modify date in the format of "yyyyMMddHHmmss"|String|
|confModAuthToken| yes|the authorization key |String|

Response

|name|description|type|
|---|---|---|
|code| return 0 if success, otherwise failed | int|
|errMsg| "OK" if success, other return error message| string|

### `admin_query_master_group_info`

Query the detail of master cluster nodes.

### `admin_transfer_current_master`

Set current master node as backup node, let it select another master.


### `groupAdmin.sh`

Clean the invalid node inside master group.

__Request__

|name|must|description|type|
|---|---|---|---|
|groupName| yes|the name of master group|String|
|helperHost| yes|the address of an online master node which will connect. The format is `ip:port`|String|
|nodeName2Remove| no|the group node to be clean|String|

Response

|name|description|type|
|---|---|---|
|code| return 0 if success, otherwise failed | int|
|errMsg| "OK" if success, other return error message| string|


### `admin_add_broker_configure`

Add broker default configuration (not include topic info). It will be effective after calling load API.

__Request__

|name|must|description|type|
|---|---|---|---|
|brokerIp|yes|a ip v4 address|string|
|brokerPort|no|the port of broker. Default is 8123 |Int|
|brokerId|yes|the id of the broker, its default value is 0. If brokerId is not zero, it ignores brokerIp field|String|
|deleteWhen|no|the default deleting time of the topic data. The format should like cronjob form `0 0 6, 18 * * ?`|String|
|deletePolicy|no|the default policy for deleting, the default policy is "delete, 168"|String|
|numPartitions|no|the default partition number of a default topic on the broker. Default 1|Int|
|unflushThreshold|no|the maximum message number which allows in memory. It has to be flushed to disk if the number exceed this value. Default 1000|Int|
|numTopicStores|no|the number of data block and partition group allowed to create, default 1. If it is larger than 1, the partition number and topic number should be mapping with this value|Int|
|unflushInterval|no|the maximum interval for unflush, default 1000ms|Int|
|memCacheMsgCntInK|no|the max cached message package, default is 10, the unit is K|Int|
|memCacheMsgSizeInMB|no|the max cache message size in MB, default 2|Int|
|memCacheFlushIntvl|no|the max unflush interval in ms, default 20000|Int|
|brokerTLSPort|no|the port of TLS of the broker, it has no default value|Int|
|acceptPublish|no|whether the broker accept publish, default true|Boolean|
|acceptSubscribe|no|whether the broker accept subscribe, default true| Boolean|
|createUser|yes|the create user|String|
|createDate|yes|the create date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_batch_add_broker_configure`

Add broker default configuration in batch (not include topic info). It will be effective after calling load API.

This API take a json string referred as `brokerJsonSet` as input parameter. The content of Json contains the configuration lists in
`admin_add_broker_configure`

__Request__

|name|must|description|type|
|---|---|---|---|
|brokerJsonSet|yes|the parameter for the configuration|String|
|createUser|yes|the creator|String|
|createDate|yes|the create date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_update_broker_configure`

Update broker default configuration (not include topic info). It will be effective after calling load API.

__Request__

|name|must|description|type|
|---|---|---|---|
|brokerId|yes|the id of the broker. It supports bulk operation by providing id set here. The brokerId should separated by `,` and be distinct|String|
|brokerPort|no|the port of broker. Default is 8123 |Int|
|deleteWhen|no|the default deleting time of the topic data. The format should like cronjob form `0 0 6, 18 * * ?`|String|
|deletePolicy|no|the default policy for deleting, the default policy is "delete, 168"|String|
|numPartitions|no|the default partition number of a default topic on the broker. Default 1|Int|
|unflushThreshold|no|the maximum message number which allows in memory. It has to be flushed to disk if the number exceed this value. Default 1000|Int|
|numTopicStores|no|the number of data block and partition group allowed to create, default 1. If it is larger than 1, the partition number and topic number should be mapping with this value|Int|
|unflushInterval|no|the maximum interval for unflush, default 1000ms|Int|
|memCacheMsgCntInK|no|the max cached message package, default is 10, the unit is K|Int|
|memCacheMsgSizeInMB|no|the max cache message size in MB, default 2|Int|
|memCacheFlushIntvl|no|the max unflush interval in ms, default 20000|Int|
|brokerTLSPort|no|the port of TLS of the broker, it has no default value|Int|
|acceptPublish|no|whether the broker accept publish, default true|Boolean|
|acceptSubscribe|no|whether the broker accept subscribe, default true| Boolean|
|modifyUser|yes|the modifier|String|
|modifyDate|yes|the modify date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_query_broker_configure`

Query the broker configuration.

__Request__

|name|must|description|type|
|---|---|---|---|
|brokerId|yes|the id of the broker. It supports bulk operation by providing id set here. The brokerId should separated by `,` and be distinct|String|
|brokerPort|no|the port of broker. Default is 8123 |Int|
|deleteWhen|no|the default deleting time of the topic data. The format should like cronjob form `0 0 6, 18 * * ?`|String|
|deletePolicy|no|the default policy for deleting, the default policy is "delete, 168"|String|
|numPartitions|no|the default partition number of a default topic on the broker. Default 1|Int|
|unflushThreshold|no|the maximum message number which allows in memory. It has to be flushed to disk if the number exceed this value. Default 1000|Int|
|numTopicStores|no|the number of data block and partition group allowed to create, default 1. If it is larger than 1, the partition number and topic number should be mapping with this value|Int|
|unflushInterval|no|the maximum interval for unflush, default 1000ms|Int|
|memCacheMsgCntInK|no|the max cached message package, default is 10, the unit is K|Int|
|memCacheMsgSizeInMB|no|the max cache message size in MB, default 2|Int|
|memCacheFlushIntvl|no|the max unflush interval in ms, default 20000|Int|
|brokerTLSPort|no|the port of TLS of the broker, it has no default value|Int|
|acceptPublish|no|whether the broker accept publish, default true|Boolean|
|acceptSubscribe|no|whether the broker accept subscribe, default true| Boolean|
|createUser|yes|the creator to be query|String|
|modifyUser|yes|the modifier to be query|String|
|topicStatusId|yes|the status of topic record|int|
|withTopic|no|whether it needs topic configuration|Boolean|

### `admin_delete_broker_configure`

Delete the broker's default configuration. It requires the related topic configuration to be delete at first, and the broker should be offline. 

__Request__

|name|must|description|type|
|---|---|---|---|
|brokerId|yes|the id of the broker. It supports bulk operation by providing id set here. The brokerId should separated by `,` and be distinct|String|
|modifyUser|yes|the modifier|String|
|modifyDate|no|the modifying date in format `yyyyMMddHHmmss`|String|
|isReserveData|no|whether to reserve production data, default false|Boolean|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_add_new_topic_record`

Add topic related configuration.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|
|brokerId|yes|the id of the broker, its default value is 0. If brokerId is not zero, it ignores brokerIp field|String|
|deleteWhen|no|the default deleting time of the topic data. The format should like cronjob form `0 0 6, 18 * * ?`|String|
|deletePolicy|no|the default policy for deleting, the default policy is "delete, 168"|String|
|numPartitions|no|the default partition number of a default topic on the broker. Default 1|Int|
|unflushThreshold|no|the maximum message number which allows in memory. It has to be flushed to disk if the number exceed this value. Default 1000|Int|
|numTopicStores|no|the number of data block and partition group allowed to create, default 1. If it is larger than 1, the partition number and topic number should be mapping with this value|Int|
|unflushInterval|no|the maximum interval for unflush, default 1000ms|Int|
|memCacheMsgCntInK|no|the max cached message package, default is 10, the unit is K|Int|
|memCacheMsgSizeInMB|no|the max cache message size in MB, default 2|Int|
|memCacheFlushIntvl|no|the max unflush interval in ms, default 20000|Int|
|brokerTLSPort|no|the port of TLS of the broker, it has no default value|Int|
|acceptPublish|no|whether the broker accept publish, default true|Boolean|
|acceptSubscribe|no|whether the broker accept subscribe, default true| Boolean|
|createUser|yes|the create user|String|
|createDate|yes|the create date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_query_topic_info`

Query specific topic record info.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|
|topicStatusId|no| the status of topic record, 0-normal record, 1-already soft delete, 2-already hard delete, default 0|int|
|brokerId|yes|the id of the broker, its default value is 0. If brokerId is not zero, it ignores brokerIp field|String|
|deleteWhen|no|the default deleting time of the topic data. The format should like cronjob form `0 0 6, 18 * * ?`|String|
|deletePolicy|no|the default policy for deleting, the default policy is "delete, 168"|String|
|numPartitions|no|the default partition number of a default topic on the broker. Default 3|Int|
|unflushThreshold|no|the maximum message number which allows in memory. It has to be flushed to disk if the number exceed this value. Default 1000|Int|
|numTopicStores|no|the number of data block and partition group allowed to create, default 1. If it is larger than 1, the partition number and topic number should be mapping with this value|Int|
|unflushInterval|no|the maximum interval for unflush, default 1000ms|Int|
|memCacheMsgCntInK|no|the max cached message package, default is 10, the unit is K|Int|
|memCacheMsgSizeInMB|no|the max cache message size in MB, default 2|Int|
|memCacheFlushIntvl|no|the max unflush interval in ms, default 20000|Int|
|brokerTLSPort|no|the port of TLS of the broker, it has no default value|Int|
|acceptPublish|no|whether the broker accept publish, default true|Boolean|
|acceptSubscribe|no|whether the broker accept subscribe, default true| Boolean|
|createUser|yes|the creator|String|
|modifyUser|yes|the modifier|String|

### `admin_modify_topic_info`

Modify specific topic record info.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|
|topicStatusId|no| the status of topic record, 0-normal record, 1-already soft delete, 2-already hard delete, default 0|int|
|brokerId|yes|the id of the broker, its default value is 0. If brokerId is not zero, it ignores brokerIp field|String|
|deleteWhen|no|the default deleting time of the topic data. The format should like cronjob form `0 0 6, 18 * * ?`|String|
|deletePolicy|no|the default policy for deleting, the default policy is "delete, 168"|String|
|numPartitions|no|the default partition number of a default topic on the broker. Default 3|Int|
|unflushThreshold|no|the maximum message number which allows in memory. It has to be flushed to disk if the number exceed this value. Default 1000|Int|
|numTopicStores|no|the number of data block and partition group allowed to create, default 1. If it is larger than 1, the partition number and topic number should be mapping with this value|Int|
|unflushInterval|no|the maximum interval for unflush, default 1000ms|Int|
|memCacheMsgCntInK|no|the max cached message package, default is 10, the unit is K|Int|
|memCacheMsgSizeInMB|no|the max cache message size in MB, default 2|Int|
|memCacheFlushIntvl|no|the max unflush interval in ms, default 20000|Int|
|brokerTLSPort|no|the port of TLS of the broker, it has no default value|Int|
|acceptPublish|no|whether the broker accept publish, default true|Boolean|
|acceptSubscribe|no|whether the broker accept subscribe, default true| Boolean|
|modifyUser|yes|the modifier|String|
|modifyDate|yes|the modification date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|


### `admin_delete_topic_info`

Delete specific topic record info softly.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|
|brokerId|yes|the id of the broker, its default value is 0. If brokerId is not zero, it ignores brokerIp field|String|
|modifyUser|yes|the modifier|String|
|modifyDate|yes|the modification date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_redo_deleted_topic_info`

Redo the Deleted specific topic record info.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|
|brokerId|yes|the id of the broker, its default value is 0. If brokerId is not zero, it ignores brokerIp field|String|
|modifyUser|yes|the modifier|String|
|modifyDate|yes|the modification date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_remove_topic_info`

Delete specific topic record info hardly.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|
|brokerId|yes|the id of the broker, its default value is 0. If brokerId is not zero, it ignores brokerIp field|String|
|modifyUser|yes|the modifier|String|
|modifyDate|yes|the modification date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_query_broker_topic_config_info`

Query the topic configuration info of the broker in current cluster.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|

## Master consumer permission operation API

### `admin_set_topic_info_authorize_control`

Enable or disable the authorization control feature of the topic. If the consumer group is not authorized, the register request will be denied.
If the topic's authorization group is empty, the topic will fail.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|
|createUser|yes|the creator|String|
|createDate|no|the creating date in format `yyyyMMddHHmmss`|String|
|isEnable|no|whether the authorization control is enable, default false|Boolean|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_delete_topic_info_authorize_control`

Delete the authorization control feature of the topic. The content of the authorized consumer group list will be delete as well.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|
|createUser|yes|the creator|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_query_topic_info_authorize_control`

Query the authorization control feature of the topic.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|
|createUser|yes|the creator|String|

### `admin_add_authorized_consumergroup_info`

Add new authorized consumer group record of the topic. The server will deny the registration from the consumer group which is not exist in
topic's authorized consumer group.


__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|
|groupName|yes| the group name to be added|String|
|createUser|yes|the creator|String|
|createDate|no|the creating date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_query_allowed_consumer_group_info`

Query the authorized consumer group record of the topic. 

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|
|groupName|yes| the group name to be added|String|
|createUser|yes|the creator|String|

### `admin_delete_allowed_consumer_group_info`

Delete the authorized consumer group record of the topic. 

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes| the topic name|String|
|groupName|yes| the group name to be added|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_batch_add_topic_authorize_control`

Add the authorized consumer group of the topic record in batch mode.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicJsonSet|yes| the topic names in JSON format|List|
|createUser|yes|the creator|String|
|createDate|no|the creating date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_batch_add_authorized_consumergroup_info`

Add the authorized consumer group record in batch mode.

__Request__

|name|must|description|type|
|---|---|---|---|
|groupNameJsonSet|yes|the group names in JSON format|List|
|createUser|yes|the creator|String|
|createDate|no|the creating date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_add_black_consumergroup_info`

Add consumer group into the black list of the topic. The registered consumer on the group cannot consume topic later as well as unregistered one.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes|the topic name |List|
|groupName|yes|the group name |List|
|createUser|yes|the creator|String|
|createDate|no|the creating date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_query_black_consumergroup_info`

Query the black list of the topic. 

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes|the topic name |List|
|groupName|yes|the group name |List|
|createUser|yes|the creator|String|

### `admin_delete_black_consumergroup_info`

Delete the black list of the topic. 

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes|the topic name |List|
|groupName|yes|the group name |List|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_add_group_filtercond_info`

Add condition of consuming filter for the consumer group 

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes|the topic name |List|
|groupName|yes|the group name |List|
|confModAuthToken|yes|the authorized key for configuration update|String|
|condStatus|no| the condition status, 0: disable, 1:enable full authorization, 2:enable and limit consuming|Int|
|filterConds|no| the filter conditions, the max length is 256|String|
|createUser|yes|the creator|String|
|createDate|no|the creating date in format `yyyyMMddHHmmss`|String|

### `admin_mod_group_filtercond_info`

Modify the condition of consuming filter for the consumer group 

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes|the topic name |List|
|groupName|yes|the group name |List|
|confModAuthToken|yes|the authorized key for configuration update|String|
|condStatus|no| the condition status, 0: disable, 1:enable full authorization, 2:enable and limit consuming|Int|
|filterConds|no| the filter conditions, the max length is 256|String|
|modifyUser|yes|the modifier|String|
|modifyDate|no|the modification date in format `yyyyMMddHHmmss`|String|

### `admin_del_group_filtercond_info`

Delete the condition of consuming filter for the consumer group 

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes|the topic name |List|
|groupName|yes|the group name |List|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_query_group_filtercond_info`

Query the condition of consuming filter for the consumer group 

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes|the topic name |List|
|groupName|yes|the group name |List|
|condStatus|no| the condition status, 0: disable, 1:enable full authorization, 2:enable and limit consuming|Int|
|filterConds|no| the filter conditions, the max length is 256|String|

### `admin_rebalance_group_allocate`

Adjust consuming partition of the specific consumer in consumer group. This includes:  \
1. release current consuming partition and retrieve new consuming partition.
2. release current consuming partition and stop consuming for a while, then retrieve new consuming partition.


__Request__

|name|must|description|type|
|---|---|---|---|
|confModAuthToken|yes|the authorized key for configuration update|String|
|consumerId|yes|the consumer id|List|
|groupName|yes|the group name |List|
|reJoinWait|no|the time in ms wait for re-consuming, the default value is 0 which means re-consuming immediately |Int|
|modifyUser|yes|the modifier|String|
|modifyDate|yes|the modification date in format `yyyyMMddHHmmss`|String|

### `admin_set_def_flow_control_rule`

Set default flow control rule. It is effective for all consumer group. It worth to note that the priority is lower than the setting in consumer group.

The flow control info is described in JSON format, for example: 

```json
[{"type":0,"rule":[{"start":"08:00","end":"17:59","dltInM":1024,"limitInM":20,"freqInMs":1000},{"start":"18:00","end":"22:00","dltInM":1024,"limitInM":20,"freqInMs":5000}]},{"type":2,"rule":[{"start":"18:00","end":"23:59","dltStInM":20480,"dltEdInMM":2048}]},{"type":1,"rule":[{"zeroCnt":3,"freqInMs":300},{"zeroCnt":8,"freqInMs":1000}]},{"type":3,"rule":[{"normFreqInMs":0,"filterFreqInMs":100,"minDataFilterFreqInMs":400}]}
```
The `type` has four values [0, 1, 2, 3]. 0: flow contorl, 1: frequency control, 2: delay to SSD storage control, 3: filter consumer frequency control,<br>
 `[start, end]` is an inclusive range of time, `dltInM` is the consuming delta in MB, `dltStInM` is consuming data delta when enabling SSD to storage <br>
`dltEdInM` is consuming data delta when terminating SSD to storage, `limitInM` is the flow control each minute, `freqInMs` is the interval for sending request
after exceeding the flow or freq limit, `zeroCnt` is the count of how many times occurs zero data, `normFreqInMs` is the interval of sequential pulling,<br>
`filterFreqInMs` is the interval of pulling filtered request.

__Request__

|name|must|description|type|
|---|---|---|---|
|flowCtrlInfo|yes|the flow control info in JSON format|String|
|confModAuthToken|yes|the authorized key for configuration update|String|
|consumerId|yes|the consumer id|List|
|groupName|yes|the group name |List|
|reJoinWait|no|the time in ms wait for re-consuming, the default value is 0 which means re-consuming immediately |Int|
|modifyUser|yes|the modifier|String|
|modifyDate|yes|the modification date in format `yyyyMMddHHmmss`|String|


### `admin_upd_def_flow_control_rule`

Update the default flow control rule.

__Request__

|name|must|description|type|
|---|---|---|---|
|confModAuthToken|yes|the authorized key for configuration update|String|
|StatusId|no| the strategy status Id, default 0|int|
|qryPriorityId|no| the consuming priority Id. It is a composed field `A0B` with default value 301, <br>the value of A,B is [1, 2, 3] which means file, backup memory, and main memory respectively|int|
|createUser|yes|the creator|String|
|needSSDProc|no|whether to enable SSD to handle, default false|Boolean|
|flowCtrlInfo|yes|the flow control info in JSON format|String|
|createDate|yes|the creating date in format `yyyyMMddHHmmss`|String|

### `admin_query_def_flow_control_rule`

Query the default flow control rule.

__Request__

|name|must|description|type|
|---|---|---|---|
|StatusId|no| the strategy status Id, default 0|int|
|qryPriorityId|no| the consuming priority Id. It is a composed field `A0B` with default value 301,<br> the value of A,B is [1, 2, 3] which means file, backup memory, and main memory respectively|int|
|createUser|yes|the creator|String|

### `admin_set_group_flow_control_rule`

Set the group flow control rule.

__Request__

|name|must|description|type|
|---|---|---|---|
|flowCtrlInfo|yes|the flow control info in JSON format|String|
|groupName|yes|the group name to set flow control rule|String|
|confModAuthToken|yes|the authorized key for configuration update|String|
|StatusId|no| the strategy status Id, default 0|int|
|qryPriorityId|no|the consuming priority Id. It is a composed field `A0B` with default value 301,<br> the value of A,B is [1, 2, 3] which means file, backup memory, and main memory respectively|int|
|createUser|yes|the creator|String|
|needSSDProc|no|whether to enable SSD to handle, default false|Boolean|
|createDate|yes|the creating date in format `yyyyMMddHHmmss`|String|

### `admin_upd_group_flow_control_rule`

Update the group flow control rule.

__Request__

|name|must|description|type|
|---|---|---|---|
|flowCtrlInfo|yes|the flow control info in JSON format|String|
|groupName|yes|the group name to set flow control rule|String|
|confModAuthToken|yes|the authorized key for configuration update|String|
|StatusId|no| the strategy status Id, default 0|int|
|qryPriorityId|no|the consuming priority Id. It is a composed field `A0B` with default value 301,<br> the value of A,B is [1, 2, 3] which means file, backup memory, and main memory respectively|int|
|createUser|yes|the creator|String|
|needSSDProc|no|whether to enable SSD to handle, default false|Boolean|
|createDate|yes|the creating date in format `yyyyMMddHHmmss`|String|


### `admin_rmv_group_flow_control_rule`

Remove the group flow control rule.

__Request__

|name|must|description|type|
|---|---|---|---|
|groupName|yes|the group name to set flow control rule|String|
|confModAuthToken|yes|the authorized key for configuration update|String|
|createUser|yes|the creator|String|

### `admin_query_group_flow_control_rule`

Remove the group flow control rule.

__Request__

|name|must|description|type|
|---|---|---|---|
|groupName|yes|the group name to set flow control rule|String|
|StatusId|no| the strategy status Id, default 0|int|
|qryPriorityId|no| the consuming priority Id. It is a composed field `A0B` with default value 301, <br>the value of A,B is [1, 2, 3] which means file, backup memory, and main memory respectively|int|
|createUser|yes|the creator|String|

### `admin_add_consume_group_setting`

Set whether to allow consume group to consume via specific offset, and the ratio of broker and client when starting the consume group.

__Request__

|name|must|description|type|
|---|---|---|---|
|groupName|yes|the group name to set flow control rule|String|
|enableBind|no|whether to bind consuming permission, default value 0 means disable|int|
|allowBCleintRatio|no|the ratio of the number of the consuming target's broker against the number of client in consuming group|int|
|createUser|yes|the creator|String|
|createDate|yes|the creating date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_query_consume_group_setting`

Query the consume group setting to check whether to allow consume group to consume via specific offset, and the ratio of broker and client when starting the consume group.

__Request__

|name|must|description|type|
|---|---|---|---|
|groupName|yes|the group name to set flow control rule|String|
|enableBind|no|whether to bind consuming permission, default value 0 means disable|int|
|allowBCleintRatio|no|the ratio of the number of the consuming target's broker against the number of client in consuming group|int|
|createUser|yes|the creator|String|

### `admin_upd_consume_group_setting`

Update the consume group setting for whether to allow consume group to consume via specific offset, and the ratio of broker and client when starting the consume group.

__Request__

|name|must|description|type|
|---|---|---|---|
|groupName|yes|the group name to set flow control rule|String|
|enableBind|no|whether to bind consuming permission, default value 0 means disable|int|
|allowBCleintRatio|no|the ratio of the number of the consuming target's broker against the number of client in consuming group|int|
|modfiyUser|yes|the modifier|String|
|modifyDate|yes|the modifying date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

### `admin_del_consume_group_setting`

Delete the consume group setting for whether to allow consume group to consume via specific offset, and the ratio of broker and client when starting the consume group.

__Request__

|name|must|description|type|
|---|---|---|---|
|groupName|yes|the group name to set flow control rule|String|
|modfiyUser|yes|the modifier|String|
|modifyDate|yes|the modifying date in format `yyyyMMddHHmmss`|String|
|confModAuthToken|yes|the authorized key for configuration update|String|

## Master subscriber relation API

1. Query consumer group subscription information

Url ` http://127.0.0.1:8088/webapi.htm?type=op_query&method=admin_query_sub_info&topicName=test&consumeGroup=xxx `

response:

```json
{
    "errCode": 0, 
    "errMsg": "Ok", 
    "count": 263,  
    "data": [{ 
        "consumeGroup": "", 
        "topicSet": ["a", "b"],
        "consumerNum": 33
       }]
}									
```

2. Query consumer group detailed subscription information

Url `http://127.0.0.1:8088/webapi.htm?type=op_query&method=admin_query_consume_group_detail&consumeGroup=test_25`

response:

```json
{
    "errCode": 0, 
    "errMsg": "Ok", 
    "count": 263, 
    "topicSet": ["a", "b"],
    "consumeGroup": "", 
    "data": [{      
       "consumerId": "",
       "parCount": 1,
       "parInfo": [{
           "brokerAddr": "",
           "topic": "",
           "partId": 2
       }] 
   }]
}									
```

## Broker operation API

### `admin_snapshot_message`

Check whether it is transferring data under current broker's topic, and what is the content.


__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes|the topic name|String|
|msgCount|no|the max number of message to extract|int|
|partitionId|yes|the partition ID which must exists|int|
|filterConds|yes|the tid value for filtering|String|

### `admin_manual_set_current_offset`

Modfiy the offset value of consuming group under current broker. The new value will be persisted to ZK.


__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes|the topic name|String|
|groupName|yes|the group name|String|
|modifyUser|no|the user who modify the value|String|
|partitionId|yes|the partition ID which must exists|int|
|manualOffset|yes|the offset to be modified, it must be a valid value|long|

### `admin_query_group_offset`

Query the offset of consuming group under current broker.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes|the topic name|String|
|groupName|yes|the group name|String|
|partitionId|yes|the partition ID which must exists|int|
|requireRealOffset|no|whether to check real offset on ZK, default false|Boolean|

### `admin_query_broker_all_consumer_info`

Query consumer info of the specific consume group on the broker.

__Request__

|name|must|description|type|
|---|---|---|---|
|groupName|yes|the group name|String|

### `admin_query_broker_all_store_info`

Query store info of the specific topic on the broker.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes|the topic name|String|

### `admin_query_broker_memstore_info`

Query memory store info of the specific topic on the broker.

__Request__

|name|must|description|type|
|---|---|---|---|
|topicName|yes|the topic name|String|
|needRefresh|no|whether it needs to refresh, default false|Boolean|

---
<a href="#top">Back to top</a>