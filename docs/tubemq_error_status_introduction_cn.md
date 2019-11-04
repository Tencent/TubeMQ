# Tube错误信息介绍

​        TubeMQ采用的是 错误码(errCode) + 错误详情(errMsg) 相结合的方式返回具体的操作结果。首先根据错误码确定是哪类问题，然后根据错误详情来确定具体的错误原因。表格汇总了所有的错误码以及运行中大家可能遇到的错误详情的相关对照。

## 错误码

| 错误类别     | 错误码                            | 错误标记                                                     | 含义                                                         | 备注                                           |
| ------------ | --------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ---------------------------------------------- |
| 成功操作     | 200                               | SUCCESS                                                      | 操作成功                                                     |                                                |
|成功操作| 201          | NOT_READY                         | 请求已接纳，但服务器还没有ready，服务还没有运行              | 保留错误，暂未使用                                           |                                                |
| 临时处理冲突 | 301                               | MOVED                                                        | 数据临时切换导致操作不成功，需要重新发起操作请求             |                                                |
| 客户端错误   | 400                               | BAD_REQUEST                                                  | 客户端侧异常，包括参数异常，状态异常等                       | 需要结合错误信息确定问题原因后重试             |
| 客户端错误| 401          | UNAUTHORIZED                      | 未授权的操作，确认客户端有权限进行该项操作                   | 需要检查配置，同时与管理员确认原因                           |                                                |
| 客户端错误| 403          | FORBIDDEN                         | 操作的Topic不存在，或者已删除                                | 需要与管理员确认具体问题原因                                 |                                                |
| 客户端错误| 404          | NOT_FOUND                         | 消费offset已经达到最大位置                                   |                                                              |                                                |
| 客户端错误| 410          | PARTITION_OCCUPIED                | 分区消费冲突，忽略即可                                       | 内部注册的临时状态，业务接口一般不会遇到该报错               |                                                |
| 客户端错误| 411          | HB_NO_NODE                        | 节点超时，需要降低操作等待一阵后再重试处理                   | 一般出现在客户端在服务器侧心跳超时，这个时候需要降低操作频率，等待一阵待lib注册成功后再重试处理 |                                                |
| 客户端错误| 412          | DUPLICATE_PARTITION               | 分区消费冲突，忽略即可                                       | 一般是由于节点超时引起，重试即可                             |                                                |
| 客户端错误| 415          | CERTIFICATE_FAILURE               | 认证失败,包括用户身份认证,以及操作授权不通过                 | 一般是用户名密码不一致,或者操作的范围未授权,需要结合错误详情进行排查 |                                                |
| 客户端错误| 419          | SERVER_RECEIVE_OVERFLOW           | 服务器接收overflow，需要重试处理                             | 如果长期的overflow，需要联系管理员扩容存储实例，或者扩大内存缓存大小 |                                                |
| 客户端错误| 450          | CONSUME_GROUP_FORBIDDEN           | 消费组被纳入黑名单                                           | 联系管理员处理                                               |                                                |
| 客户端错误| 452          | SERVER_CONSUME_SPEED_LIMIT        | 消费被限速                                                   | 联系管理员处理，解除限速                                     |                                                |
| 客户端错误| 455          | CONSUME_CONTENT_FORBIDDEN         | 消费内容拒绝，包括消费组禁止过滤消费，过滤的tid集合与允许的tid集合不一致等 | 对齐过滤消费的设置确认没有问题后，再联系管理员处理           |                                                |
| 服务器侧异常 | 500                               | INTERNAL_SERVER_ERROR                                        | 内部服务器错误                                               | 需要结合错误信息，联系管理员确定问题原因后重试 |
| 服务器侧异常| 503          | SERVICE_UNAVILABLE                | 业务临时禁读或者禁写                                         | 继续重试处理，如果持续的出现该类错误，需要联系管理员处理     |                                                |
| 服务器侧异常| 510          | INTERNAL_SERVER_ERROR_MSGSET_NULL | 读取不到消息集合                                             | 继续重试处理，如果持续的出现该类错误，需要联系管理员处理     |                                                |

## 常见错误信息

| 记录号 | 错误信息                                                     | 含义                                                         | 备注                                                         |
| ------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1      | Status error: producer has been   shutdown!                  | 客户端已停止                                                 |                                                              |
| 2      | Illegal parameter: blank topic!                              | 参数错误: 空的topic                                          |                                                              |
| 3      | Illegal parameter: topicSet is   null or empty!              | 参数错误: 空的topic集合                                      |                                                              |
| 4      | Illegal parameter: found blank   topic value in topicSet : xxxxx | 参数错误: topic集合里含有空的topic                           |                                                              |
| 5      | Send message failed                                          | 消息发送失败                                                 |                                                              |
| 6      | Illegal parameter: null message   package!                   | 空消息包                                                     |                                                              |
| 7      | Illegal parameter: null data in   message package!           | 空消息内容                                                   |                                                              |
| 8      | Illegal parameter: over max   message length for the total size of message data and attribute, allowed size   is XX，message's real size is YY | 消息长度超过指定长                                           |                                                              |
| 9      | Topic XX not publish, please   publish first!                | topic没有发布                                                |                                                              |
| 10     | Topic XX not publish, make sure   the topic exist or acceptPublish and try later! | topic没有发布，或者发布的topic不存在                         |                                                              |
| 11     | Null partition for topic: XX ,   please try later!           | 对应topic暂时没有分配到分区                                  |                                                              |
| 12     | No available partition for topic:   XX                       | 没有有效分区                                                 |                                                              |
| 13     | Current delayed messages over max   allowed count, allowed is xxxxx, current count is yyyy | 当前滞留未响应的消息超过了允许值                             | 稍后再发，该值缺省40W，通过TubeClientConfig.setSessionMaxAllowedDelayedMsgCount()控制 |
| 14     | The brokers of topic are all   forbidden!                    | 对应Topic的Broker均因为网络质量问题被屏蔽                    | 稍后再发，待屏蔽策略解除                                     |
| 15     | Not found available partition for   topic: XX                | 没有找到有效分区                                             | 有分区分配但由于网络质量问题分配的分区处于屏蔽中，           |
| 16     | Channel is not writable, please   try later!                 | 管道不可写                                                   | 缺省10M，通过TubeClientConfig.setNettyWriteBufferHighWaterMark调大写buffer |
| 17     | Put message failed from xxxxx,   server receive message overflow! | 存储消息时对应服务器过载                                     | 重试消息发送,若持续的出现该类报错,则需要联系管理员进行扩容处理 |
| 18     | Write StoreService temporary   unavailable!                   | 对应服务器临时写无效                                         | 重试消息发送，如果连续写无效，联系管理员，调整分区在broker上的分布，同时处理异常的broker |
| 19     | Topic xxx not existed, please   check your configure         | 生产的Topic不存在                                            | 有可能是生产过程中topic被管理员删除,联系管理员处理           |
| 20     | Partition[xxx:yyy] has been closed                           | 生产的Topic已删除                                            | 有可能是生产过程中topic被管理员软删除,联系管理员处理         |
| 21     | Partition xxx-yyy not existed,   please check your configure | 生产的Topic分区不存在                                        | 异常情况,分区只会增加不会减少,联系管理员处理                 |
| 22     | Checksum failure xxx of yyyy not   equal to the data's checksum | 生产的数据checksum计算结果不一致                             | 异常情况,携带的内容计算的checksum计算错误,或者传输途中被篡改 |
| 23     | Put message failed from xxxxx                                | 存储消息失败                                                 | 重发,同时将错误信息发给管理员确认问题原因                    |
| 24     | Put message failed2 from                                     | 存储消息失败2                                                | 重发,同时将错误信息发给管理员确认问题原因                    |
| 25     | Null brokers to select sent,   please try later!             | 当前无可用的Broker进行消息发送                               | 等待一阵时间再重试，如果一直如此联系管理员，该情况有可能是broker异常，或者未完成消息过多引起，需要查看Broker状态后确定。 |
| 26     | Publish topic failure, make sure   the topic xxx exist or acceptPublish and try later! | publish topic失败，确认该topic存在或者处于可写状态           | 调用void publish(final String   topic)接口时如果对应topic还不在本地，或者topic不存在时将报该错误，等待1分钟左右，或者使用Set<String>   publish(Set<String> topicSet)接口完成topic的发布。 |
| 27     | Register producer failure,   response is null!               | 注册producer失败                                             | 需要联系管理员处理                                           |
| 28     | Register producer failure, error   is  XXX                   | 注册producer失败，错误原因是 XXX                             | 根据错误原因进行问题核对，如果仍错误，联系管理员处理。       |
| 29     | Register producer exception, error   is XXX                  | 注册producer发生异常，错误原因是 XXX                         | 根据错误原因进行问题核对，如果仍错误，联系管理员处理。       |
| 30     | Status error: please call start   function first!            | 需要首先调用start函数                                        | 接口使用问题，Producer不是从sessionFactory创建生成，调用sessionfactory里的createProducer()函数进行创建后再使用 |
| 31     | Status error: producer service has   been shutdown!          | producer服务已经停止                                         | producer已经停止服务，停止业务函数调用                       |
| 32     | Listener is null for topic XXX                               | 针对topic XXX传递的回调对象Listener为null                    | 输入参数不合法，需要检查业务代码                             |
| 33     | Please complete topic's Subscribe   call first!              | 请先完成对应topic的subscribe()函数调用                       | 接口使用问题，需要先完成topic的订阅操作再进行消费消费处理    |
| 34     | ConfirmContext is null !                                     | 空ConfirmContext内容，非法上下文                             | 需要业务确认接口调用逻辑                                     |
| 35     | ConfirmContext format error: value   must be aaaa:bbbb:cccc:ddddd ! | ConfirmContext内容的格式不正确                               | 需要业务确认接口调用逻辑                                     |
| 36     | ConfirmContext's format error:   item (XXX) is null !        | ConfirmContext内容有异常，存在Blank内容                      | 需要业务确认接口调用逻辑                                     |
| 37     | The confirmContext's value   invalid!                        | 无效ConfirmContext内容                                       | 有可能是不存在的上下文，或者因为负载均衡对应分区已释放上下文已经过期 |
| 38     | Confirm XXX 's offset failed!                                | confirm offset失败                                           | 需要根据日志详情确认问题原因，如果持续的出现该问题，联系管理员处理。 |
| 39     | Not found the partition by   confirmContext:XXX              | 确认的partition未找到                                        | 服务端负载均衡对应分区已释放                                 |
| 40     | Illegal parameter:   messageSessionFactory or consumerConfig is null! | messageSessionFactory或者   consumerConfig为null             | 检查对象初始化逻辑，确认配置的准确性                         |
| 41     | Get consumer id failed!                                      | consumer的唯一识别ID生成失败                                 | 持续失败时，将异常堆栈信息给到系统管理员处理                 |
| 42     | Parameter error: topic is Blank!                             | 输入的topic为Blank                                           | Blank包括参数为null，输入的参数不为空，但内容长度为0，或者内容为isWhitespace字符 |
| 43     | Parameter error: Over max allowed   filter count, allowed count is XXX | 过滤项个数超过系统允许的最大量                               | 参数异常，调整个数                                           |
| 44     | Parameter error: blank filter   value in parameter filterConds! | filterConds里包含了为Blank的内容项                           | 参数异常，调整内容项值                                       |
| 45     | Parameter error: over max allowed   filter length, allowed length is XXX | 过滤项长度超标                                               |                                                              |
| 46     | Parameter error: null   messageListener                      | messageListener参数为null                                    |                                                              |
| 47     | Topic=XXX has been subscribed                              | Topic XXX被重复订阅                                          |                                                              |
| 48     | Not subscribe any topic, please   subscribe first!           | 未订阅任何topic即启动消费                                    | 接口调用逻辑异常，检查业务代码                               |
| 49     | Duplicated completeSubscribe call!                           | 重复调用completeSubscribe()函数                              | 接口调用逻辑异常，检查业务代码                               |
| 50     | Subscribe has finished!                                      | 重复调用completeSubscribe()函数                              |                                                              |
| 51     | Parameter error: sessionKey is   Blank!                      | 参数不合规，sessionKey值不允许为Blank内容                    |                                                              |
| 52     | Parameter error: sourceCount must   over zero!               | 参数不合规，sourceCount值必须大于0                           |                                                              |
| 53     | Parameter error: partOffsetMap's   key XXX format error: value must be aaaa:bbbb:cccc ! | 参数不合规，partOffsetMap的key值内容必须是aaaa:bbbb:cccc格式 |                                                              |
| 54     | Parameter error: not included in   subscribed topic list: partOffsetMap's key is XXX , subscribed topics are YYY | 参数不合规，partOffsetMap里指定的topic在订阅列表里并不存在   |                                                              |
| 55     | Parameter error: illegal format   error of XXX  : value must not include   ',' char!" | 参数不合规，key值里面不能包含","字符                         |                                                              |
| 56     | Parameter error: Offset must over   or equal zero of partOffsetMap  key   XXX, value is YYY | 参数不合规，offset值必须是大于等于0                          |                                                              |
| 57     | Duplicated completeSubscribe call!                           | 重复调用completeSubscribe()函数                              |                                                              |
| 58     | Register to master failed!   ConsumeGroup forbidden, XXX     | 注册Master失败，消费组被禁止                                 | 服务端主动禁止行为，联系系统管理员处理                       |
| 59     | Register to master failed!   Restricted consume content, XXX | 注册Master失败，消费内容受限                                 | 过滤消费的tid集合不在申请的集合范围内                        |
| 60     | Register to master failed! please   check and retry later.   | 注册Master失败，请重试                                       | 这种情况需要查看客户端日志，确认问题原因，在核实没有异常日志，同时master地址填写正确，联系系统管理员处理。 |
| 61     | Get message error, reason is XXX                             | 因为XXX原因拉取消息失败                                      | 确定下问题原因，将相关错误信息提交给相关的业务负责人处理，需要根据具体错误信息对齐原因 |
| 62     | Get message null                                             | 获取到的消息为null                                           | 重试                                                         |
| 63     | Get message   failed,topic=XXX,partition=YYY, throw info is ZZZ | 拉取消息失败                                                 | 将相关错误信息提交给相关的业务负责人处理，需要根据具体错误信息对齐原因 |
| 64     | Status error: consumer has been   shutdown                   | 消费者已调用shutdown，不应该继续调用其它函数进行业务处理     |                                                              |
| 65     | All partition in waiting, retry   later!                      | 所有分区都在等待,请稍候                                      | 该错误信息可以不做打印,遇到该情况时拉取县城sleep 200 ~   400ms |
| 66     | The request offset reached   maxOffset                       | 请求的分区已经消费到最新位置                                 | 可以通过ConsumerConfig.setMsgNotFoundWaitPeriodMs()设置该情况时分区停止拉取的时间段来等待最新消息的到来 |

---
<a href="#top">Back to top</a>