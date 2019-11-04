# TubeMQ configuration item description

The TubeMQ server includes two modules for the Master and the Broker. The Master also includes a Web front-end module for external page access (this part is stored in the resources). Considering the actual deployment, two modules are often deployed in the same machine, TubeMQ. The contents of the three parts of the two modules are packaged and delivered to the operation and maintenance; the client does not include the lib package of the server part and is delivered to the user separately.

Master and Broker use the ini configuration file format, and the relevant configuration files are placed in the master.ini and broker.ini files in the tubemq-server-3.8.0/conf/ directory.

Their configuration is defined by a set of configuration units. The Master configuration consists of four mandatory units: [master], [zookeeper], [bdbStore], and optional [tlsSetting]. The Broker configuration is mandatory. Broker], [zookeeper] and optional [tlsSetting] consist of a total of 3 configuration units; in actual use, you can also combine the contents of the two configuration files into one ini file.

In addition to the back-end system configuration file, the Master also stores the Web front-end page module in the resources. The root directory velocity.properties file of the resources is the Web front-end page configuration file of the Master.



## Configuration item details:

### master.ini file:
[master]
> Master system runs the main configuration unit, required unit, the value is fixed to "[master]"

| Name                          | Required                          | Type                          | Description                                                  |
| ----------------------------- |  ----------------------------- |  ----------------------------- | ------------------------------------------------------------ |
| hostName                      | yes      | string  | The host address of the master external service, required, must be configured on the NIC, is enabled, non-loopback and cannot be IP of 127.0.0.1 |
| port                          | no       | int     | Master listening port, optional, default is 8715             |
| webPort                       | no       | int     | Master web console access port, the default value is 8080    |
| webResourcePath               | yes      | string  | Master Web Resource deploys an absolute path, which is required. If the value is set incorrectly, the web page will not display properly. |
| confModAuthToken              | no       | string  | The authorization Token provided by the operator when the change operation (including adding, deleting, changing configuration, and changing the master and managed Broker status) is performed by the Master's Web or API. The value is optional. The default is "ASDFGHJKL". |
| firstBalanceDelayAfterStartMs | no       | long    | Master starts to the interval of the first time to start Rebalance, optional, default 30000 milliseconds |
| consumerBalancePeriodMs       | no       | long    | The master balances the rebalance period of the consumer group. The default is 60000 milliseconds. When the cluster size is large, increase the value. |
| consumerHeartbeatTimeoutMs    | no       | long    | Consumer heartbeat timeout period, optional, default 30000 milliseconds, when the cluster size is large, please increase the value |
| producerHeartbeatTimeoutMs    | no       | long    | Producer heartbeat timeout period, optional, default 30000 milliseconds, when the cluster size is large, please increase the value |
| brokerHeartbeatTimeoutMs      | no       | long    | Broker heartbeat timeout period, optional, default 30000 milliseconds, when the cluster size is large, please increase the value |
| socketRecvBuffer              | no       | long    | Socket receives the size of the Buffer buffer SO_RCVBUF, the unit byte, the negative number is set as the default value |
| socketSendBuffer              | no       | long    | Socket sends Buffer buffer SO_SNDBUF size, unit byte, negative number is  set as the default value |
| maxAutoForbiddenCnt           | no       | int     | When the broker has an IO failure, the maximum number of masters allowed to automatically go offline is the number of options. The default value is 5. It is recommended that the value does not exceed 10% of the total number of brokers in the cluster. |
| startOffsetResetCheck         | no       | boolean | Whether to enable the check function of the client Offset reset function, optional, the default is false |
| needBrokerVisitAuth           | no       | boolean | Whether to enable Broker access authentication, the default is false. If true, the message reported by the broker must carry the correct username and signature information. |
| visitName                     | no       | string  | The username of the Broker access authentication. The default is an empty string. This value must exist when needBrokerVisitAuth is true. This value must be the same as the value of the visitName field in broker.ini. |
| visitPassword                 | no       | string  | The password for the Broker access authentication. The default is an empty string. This value must exist when needBrokerVisitAuth is true. This value must be the same as the value of the visitPassword field in broker.ini. |
| startProduceAuthenticate      | no       | boolean | Whether to enable production end user authentication, the default is false |
| startProduceAuthorize         | no       | boolean | Whether to enable production-side production authorization authentication, the default is false |
| startConsumeAuthenticate      | no       | boolean | Whether to enable consumer user authentication, the default is false |
| startConsumeAuthorize         | no       | boolean | Whether to enable consumer consumption authorization authentication, the default is false |
| maxGroupBrokerConsumeRate     | no       | int     | The maximum ratio of the number of clustered brokers to the number of members in the consumer group. The default is 50. In a 50-kerrow cluster, one consumer group is allowed to start at least one client. |

[zookeeper]
>The corresponding Tom MQ cluster of the Master stores the information about the ZooKeeper cluster of the Offset. The required unit has a fixed value of "[zookeeper]".

| Name                  | Required                          | Type                          | Description                                                  |
| --------------------- |  -----------------------------|  ----------------------------- | ------------------------------------------------------------ |
| zkServerAddr          | no       | string | Zk server address, optional configuration, defaults to "localhost:2181" |
| zkNodeRoot            | no       | string | The root path of the node on zk, optional configuration. The default is "/tube". |
| zkSessionTimeoutMs    | no       | long   | Zk heartbeat timeout, in milliseconds, default 30 seconds    |
| zkConnectionTimeoutMs | no       | long   | Zk connection timeout, in milliseconds, default 30 seconds   |
| zkSyncTimeMs          | no       | long   | Zk data synchronization time, in milliseconds, default 5 seconds |
| zkCommitPeriodMs      | no       | long   | The interval at which the Master cache data is flushed to zk, in milliseconds, default 5 seconds. |

[bdbStore]
>Master configuration of the BDB cluster to which the master belongs. The master uses BDB for metadata storage and multi-node hot standby. The required unit has a fixed value of "[bdbStore]".

| Name                    | Required                          | Type                          | Description                                                  |
| ----------------------- |  ----------------------------- |  ----------------------------- | ------------------------------------------------------------ |
| bdbRepGroupName         | yes      | string | BDB cluster name, the primary and backup master node values must be the same, required field |
| bdbNodeName             | yes      | string | The name of the node of the master in the BDB cluster. The value of each BDB node must not be repeated. Required field. |
| bdbNodePort             | no       | int    | BDB node communication port, optional field, default is 9001 |
| bdbEnvHome              | yes      | string | BDB data storage path, required field                        |
| bdbHelperHost           | yes      | string | Primary node when the BDB cluster starts, required field     |
| bdbLocalSync            | no       | int    | BDB data node local storage mode, the value range of this field is [1, 2, 3]. The default is 1: 1 is data saved to disk, 2 is data only saved to memory, and 3 is only data is written to file system buffer. But not brush |
| bdbReplicaSync          | no       | int    | BDB data node synchronization save mode, the value range of this field is [1, 2, 3]. The default is 1: 1 is data saved to disk, 2 is data only saved to memory, and 3 is only data is written to file system buffer. But not brush |
| bdbReplicaAck           | no       | int    | The response policy of the BDB node data synchronization, the value range of this field is [1, 2, 3], the default is 1: 1 is more than 1/2 majority is valid, 2 is valid for all nodes, 3 is not Need node response |
| bdbStatusCheckTimeoutMs | no       | long   | BDB status check interval, optional field, in milliseconds, defaults to 10 seconds |

[tlsSetting]
>The Master uses TLS to encrypt the transport layer data. When TLS is enabled, the configuration unit provides related settings. The optional unit has a fixed value of "[tlsSetting]".

| Name                  | Required                          | Type                          | Description                                                  |
| --------------------- |  -----------------------------|  ----------------------------- | ------------------------------------------------------------ |
| tlsEnable             | no       | boolean | Whether to enable TLS function, optional configuration, default is false |
| tlsPort               | no       | int     | Master TLS port number, optional configuration, default is 8716 |
| tlsKeyStorePath       | no       | string  | The absolute storage path of the TLS keyStore file + the name of the keyStore file. This field is required and cannot be empty when the TLS function is enabled. |
| tlsKeyStorePassword   | no       | string  | The absolute storage path of the TLS keyStorePassword file + the name of the keyStorePassword file. This field is required and cannot be empty when the TLS function is enabled. |
| tlsTwoWayAuthEnable   | no       | boolean | Whether to enable TLS mutual authentication, optional configuration, the default is false |
| tlsTrustStorePath     | no       | string  | The absolute storage path of the TLS TrustStore file + the TrustStore file name. This field is required and cannot be empty when the TLS function is enabled and mutual authentication is enabled. |
| tlsTrustStorePassword | no       | string  | The absolute storage path of the TLS TrustStorePassword file + the TrustStorePassword file name. This field is required and cannot be empty when the TLS function is enabled and mutual authentication is enabled. |

### velocity.properties file:

| Name                      | Required                          | Type                          | Description                                                  |
| ------------------------- |  ----------------------------- |  ----------------------------- | ------------------------------------------------------------ |
| file.resource.loader.path | yes      | string | The absolute path of the master web template. This part is the absolute path plus /resources/templates of the project when the master is deployed. The configuration is consistent with the actual deployment. If the configuration fails, the master front page access fails. |

### broker.ini file:

[broker]
>The broker system runs the main configuration unit, required unit, and the value is fixed to "[broker]"

| Name                  | Required                          | Type                          | Description                                                  |
| --------------------- |  ----------------------------- |  ----------------------------- | ------------------------------------------------------------ |
| brokerId              | yes      | int     | Server unique flag, required field, can be set to 0; when set to 0, the system will default to take the local IP to int value |
| hostName              | yes      | string  | The host address of the broker external service, required, must be configured in the NIC, is enabled, non-loopback and cannot be IP of 127.0.0.1 |
| port                  | no       | int     | Broker listening port, optional, default is 8123             |
| webPort               | no       | int     | Broker's http management access port, optional, default is 8081 |
| masterAddressList     | yes      | string  | Master address list of the cluster to which the broker belongs. Required fields. The format must be ip1:port1, ip2:port2, ip3:port3. |
| primaryPath           | yes      | string  | Broker stores the absolute path of the message, mandatory field |
| maxSegmentSize        | no       | int     | Broker stores the file size of the message data content, optional field, default 512M, maximum 1G |
| maxIndexSegmentSize   | no       | int     | Broker stores the file size of the message Index content, optional field, default 18M, about 70W messages per file |
| transferSize          | no       | int     | Broker allows the maximum message content size to be transmitted to the client each time, optional field, default is 512K |
| consumerRegTimeoutMs  | no       | long    | Consumer heartbeat timeout, optional, in milliseconds, default 30 seconds |
| socketRecvBuffer      | no       | long    | Socket receives the size of the Buffer buffer SO_RCVBUF, the unit byte, the negative number is not set, the default value is |
| socketSendBuffer      | no       | long    | Socket sends Buffer buffer SO_SNDBUF size, unit byte, negative number is not set, the default value is |
| secondDataPath        | no       | string  | The SSD to storage location where the broker is located, optional field. The default is blank to indicate that the machine has no SSD. |
| maxSSDTotalFileCnt    | no       | int     | The maximum number of Data files allowed by the SSD where the Broker is located, optional field, default 70 |
| maxSSDTotalFileSizes  | no       | long    | The SSD where the Broker is located allows the maximum size of the data file to be saved. The optional field is 32G by default. |
| tcpWriteServiceThread | no       | int     | Broker supports the number of socket worker threads for TCP production services, optional fields, and defaults to 2 times the number of CPUs of the machine. |
| tcpReadServiceThread  | no       | int     | Broker supports the number of socket worker threads for TCP consumer services, optional fields, defaults to 2 times the number of CPUs of the machine |
| logClearupDurationMs  | no       | long    | The aging cleanup period of the message file, in milliseconds. The default is 30 minutes for a log cleanup operation. The minimum is 30 minutes. |
| logFlushDiskDurMs     | no       | long    | Batch check message persistence to file check cycle, in milliseconds, default is 20 seconds for a full check and brush |
| visitMasterAuth       | no       | boolean | Whether the authentication of the master is enabled, the default is false. If true, the user name and signature information are added to the signaling reported to the master. |
| visitName             | no       | string  | User name of the access master. The default is an empty string. This value must exist when visitMasterAuth is true. The value must be the same as the value of the visitName field in master.ini. |
| visitPassword         | no       | string  | The password for accessing the master. The default is an empty string. This value must exist when visitMasterAuth is true. The value must be the same as the value of the visitPassword field in master.ini. |
| logFlushMemDurMs      | no       | long    | Batch check message memory persistence to file check cycle, in milliseconds, default is 10 seconds for a full check and brush |

[zookeeper]
>The Tube MQ cluster corresponding to the Broker stores the information about the ZooKeeper cluster of the Offset. The required unit has a fixed value of "[zookeeper]".


| Name                  | Required                          | Type                          | Description                                                  |
| --------------------- |  ----------------------------- |  ----------------------------- | ------------------------------------------------------------ |
| zkServerAddr          | no       | string | Zk server address, optional configuration, defaults to "localhost:2181" |
| zkNodeRoot            | no       | string | The root path of the node on zk, optional configuration. The default is "/tube". |
| zkSessionTimeoutMs    | no       | long   | Zk heartbeat timeout, in milliseconds, default 30 seconds    |
| zkConnectionTimeoutMs | no       | long   | Zk connection timeout, in milliseconds, default 30 seconds   |
| zkSyncTimeMs          | no       | long   | Zk data synchronization time, in milliseconds, default 5 seconds |
| zkCommitPeriodMs      | no       | long   | The interval at which the broker cache data is flushed to zk, in milliseconds, default 5 seconds |
| zkCommitFailRetries   | no       | int    | The maximum number of re-brushings after Broker fails to flush cached data to Zk |

[tlsSetting]
>The Master uses TLS to encrypt the transport layer data. When TLS is enabled, the configuration unit provides related settings. The optional unit has a fixed value of "[tlsSetting]".


| Name                  | Required                          | Type                           | Description                                                  |
| --------------------- |  ----------------------------- |  ----------------------------- | ------------------------------------------------------------ |
| tlsEnable             | no       | boolean | Whether to enable TLS function, optional configuration, default is false |
| tlsPort               | no       | int     | Broker TLS port number, optional configuration, default is 8124 |
| tlsKeyStorePath       | no       | string  | The absolute storage path of the TLS keyStore file + the name of the keyStore file. This field is required and cannot be empty when the TLS function is enabled. |
| tlsKeyStorePassword   | no       | string  | The absolute storage path of the TLS keyStorePassword file + the name of the keyStorePassword file. This field is required and cannot be empty when the TLS function is enabled. |
| tlsTwoWayAuthEnable   | no       | boolean | Whether to enable TLS mutual authentication, optional configuration, the default is false |
| tlsTrustStorePath     | no       | string  | The absolute storage path of the TLS TrustStore file + the TrustStore file name. This field is required and cannot be empty when the TLS function is enabled and mutual authentication is enabled. |
| tlsTrustStorePassword | no       | string  | The absolute storage path of the TLS TrustStorePassword file + the TrustStorePassword file name. This field is required and cannot be empty when the TLS function is enabled and mutual authentication is enabled. |

---
<a href="#top">Back to top</a>