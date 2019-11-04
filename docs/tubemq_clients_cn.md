## **TubeMQ Lib** **接口使用**

------



### **1. 基础对象接介绍：**

#### **a) MessageSessionFactory（消息会话工厂）：**

TubeMQ 采用MessageSessionFactory（消息会话工厂）来管理网络连接，又根据业务不同客户端是否复用连接细分为TubeSingleSessionFactory（单连接会话工厂）类和TubeMultiSessionFactory（多连接会话工厂）类2个部分，其实现逻辑大家可以从代码可以看到，单连接会话通过定义clientFactory静态类，实现了进程内不同客户端连接相同目标服务器时底层物理连接只建立一条的特征，多连接会话里定义的clientFactory为非静态类，从而实现同进程内通过不同会话工厂，创建的客户端所属的连接会话不同建立不同的物理连接。通过这种构造解决连接创建过多的问题，业务可以根据自身需要可以选择不同的消息会话工厂类，一般情况下我们使用单连接会话工厂类。

 

#### **b) MasterInfo:**

TubeMQ的Master地址信息对象，该对象的特点是支持配置多个Master地址，由于TubeMQ Master借助BDB的存储能力进行元数据管理，以及服务HA热切能力，Master的地址相应地就需要配置多条信息。该配置信息支持IP、域名两种模式，由于TubeMQ的HA是热切模式，客户端要保证到各个Master地址都是连通的。该信息在初始化TubeClientConfig类对象和ConsumerConfig类对象时使用，考虑到配置的方便性，我们将多条Master地址构造成“ip1:port1,ip2:port2,ip3:port3”格式并进行解析。

 

#### **c) TubeClientConfig：**

MessageSessionFactory（消息会话工厂）初始化类，用来携带创建网络连接信息、客户端控制参数信息的对象类，包括RPC时长设置、Socket属性设置、连接质量检测参数设置、TLS参数设置、认证授权信息设置等信息，该类，连同接下来介绍的ConsumerConfig类，与TubeMQ-3.8.0版本之前版本的类变更最大的类，主要原因是在此之前TubeMQ的接口定义超6年多没有变更，接口使用上存在接口语义定义有歧义、接口属性设置单位不清晰、程序无法识别多种情况的内容选择等问题，考虑到代码开源自查问题方便性，以及新手学习成本问题，我们这次作了接口的重定义。对于重定义的前后差别，见配置接口定义说明部分介绍。

 

#### **d) ConsumerConfig：**

ConsumerConfig类是TubeClientConfig类的子类，它是在TubeClientConfig类基础上增加了Consumer类对象初始化时候的参数携带，因而在一个既有Producer又有Consumer的MessageSessionFactory（消息会话工厂）类对象里，会话工厂类的相关设置以MessageSessionFactory类初始化的内容为准，Consumer类对象按照创建时传递的初始化类对象为准。在consumer里又根据消费行为的不同分为Pull消费者和Push消费者两种，两种特有的参数通过参数接口携带“pull”或“push”不同特征进行区分。

 

#### **e) Message：**

Message类是TubeMQ里传递的消息对象类，业务设置的data会从生产端原样传递给消息接收端，attribute内容是与TubeMQ系统共用的字段，业务填写的内容不会丢失和改写，但该字段有可能会新增TubeMQ系统填写的内容，并在后续的版本中，新增的TubeMQ系统内容有可能去掉而不被通知。该部分需要注意的是Message.putSystemHeader(final String msgType, final String msgTime)接口，该接口用来设置消息的消息类型和消息发送时间，msgType用于消费端过滤用，msgTime用做TubeMQ进行数据收发统计时消息时间统计维度用。

 

#### **f) MessageProducer：**

消息生产者类，该类完成消息的生产，消息发送分为同步发送和异步发送两种接口，目前消息采用Round Robin方式发往后端服务器，后续这块将考虑按照业务指定的算法进行后端服务器选择方式进行生产。该类使用时需要注意的是，我们支持在初始化时候全量Topic指定的publish，也支持在生产过程中临时增加对新的Topic的publish，但临时增加的Topic不会立即生效，因而在使用新增Topic前，要先调用isTopicCurAcceptPublish接口查询该Topic是否已publish并且被服务器接受，否则有可能消息发送失败。

 

#### **g) MessageConsumer：**

该类有两个子类PullMessageConsumer、PushMessageConsumer，通过这两个子类的包装，完成了对业务侧的Pull和Push语义。实际上TubeMQ是采用Pull模式与后端服务进行交互，为了便于业务的接口使用，我们进行了封装，大家可以看到其差别在于Push在启动时初始化了一个线程组，来完成主动的数据拉取操作。需要注意的地方在于：

- a. CompleteSubscribe接口，带参数的接口支持客户端对指定的分区进行指定offset消费，不带参数的接口则按照ConsumerConfig.setConsumeModel(int consumeModel)接口进行对应的消费模式设置来消费数据;
	
- b. 对subscribe接口，其用来定义该消费者的消费目标，而filterConds参数表示对待消费的Topic是否进行过滤消费，以及如果做过滤消费时要过滤的msgType消息类型值。如果不需要进行过滤消费，则该参数填为null，或者空的集合值。

 

------



### **2. 接口调用示例：**

#### **a) 环境准备：**

TubeMQ开源包com.tencent.tube.example里提供了生产和消费的具体代码示例，这里我们通过一个实际的例子来介绍如何填参和调用对应接口。首先我们搭建一个带3个Master节点的TubeMQ集群，3个Master地址及端口分别为test_1.domain.com，test_2.domain.com，test_3.domain.com，端口均为8080，在该集群里我们建立了若干个Broker，并且针对Broker我们创建了3个topic：topic_1，topic_2，topic_3等Topic配置；然后我们启动对应的Broker等待Consumer和Producer的创建。

 

#### **b) 创建Consumer：**

见包com.tencent.tubemq.example.MessageConsumerExample类文件，Consumer是一个包含网络交互协调的客户端对象，需要做初始化并且长期驻留内存重复使用的模型，它不适合单次拉起消费的场景。如下图示，我们定义了MessageConsumerExample封装类，在该类中定义了进行网络交互的会话工厂MessageSessionFactory类，以及用来做Push消费的PushMessageConsumer类：

- ###### **i.初始化MessageConsumerExample类：**

1. 首先构造一个ConsumerConfig类，填写初始化信息，包括本机IP V4地址，Master集群地址，消费组组名信息，这里Master地址信息传入值为：”test_1.domain.com:8080,test_2.domain.com:8080,test_3.domain.com:8080”；

2. 然后设置消费模式：我们设置首次从队列尾消费，后续接续消费模式；

3. 然后设置Push消费时回调函数个数

4. 进行会话工厂初始化操作：该场景里我们选择建立单链接的会话工厂；

5. 在会话工厂创建模式的消费者：

```java
public final class MessageConsumerExample {
	private static final Logger logger = 
        LoggerFactory.getLogger(MessageConsumerExample.class);
    private static final MsgRecvStats msgRecvStats = new MsgRecvStats();
    private final String masterHostAndPort;
    private final String localHost;
    private final String group;
    private PushMessageConsumer messageConsumer;
    private MessageSessionFactory messageSessionFactory;
    
    public MessageConsumerExample(String localHost,
                                  String masterHostAndPort,
                                  String group,
                                  int fetchCount) throws Exception {
        this.localHost = localHost;
        this.masterHostAndPort = masterHostAndPort;
        this.group = group;
        ConsumerConfig consumerConfig = 
            new ConsumerConfig(this.localHost,this.masterHostAndPort, this.group);
        consumerConfig.setConsumeModel(0);
        if (fetchCount > 0) {
            consumerConfig.setPushFetchThreadCnt(fetchCount);
        }
        this.messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        this.messageConsumer = messageSessionFactory.createPushConsumer(consumerConfig);
    }
}
```



- ###### **ii.订阅Topic：**

我们没有采用指定Offset消费的模式进行订阅，也没有过滤需求，因而我们在如下代码里只做了Topic的指定，对应的过滤项集合我们传的是null值，同时，对于不同的Topic，我们可以传递不同的消息回调处理函数；我们这里订阅了3个topic，topic_1，topic_2，topic_3，每个topic分别调用subscribe函数进行对应参数设置：

```java
public void subscribe(final Map<String, TreeSet<String>> topicTidsMap)
    throws TubeClientException {
    for (Map.Entry<String, TreeSet<String>> entry : topicTidsMap.entrySet()) {
        this.messageConsumer.subscribe(entry.getKey(),
                                       entry.getValue(), 
                                       new DefaultMessageListener(entry.getKey()));
    }
    messageConsumer.completeSubscribe();
}
```



- ###### **iii.进行消费：**

到此，对集群里对应topic的订阅就已完成，系统运行开始后，回调函数里数据将不断的通过回调函数推送到业务层进行处理：

```java
public class DefaultMessageListener implements MessageListener {

    private String topic;

    public DefaultMessageListener(String topic) {
        this.topic = topic;
    }

    public void receiveMessages(final List<Message> messages) throws InterruptedException 
    {
        if (messages != null && !messages.isEmpty()) {
            msgRecvStats.addMsgCount(this.topic, messages.size());
        }
    }

    public Executor getExecutor() {
        return null;
    }

    public void stop() {
    }
}
```



#### **c) 创建Producer：**

现网环境中业务的数据都是通过代理层来做接收汇聚，包装了比较多的异常处理，大部分的业务都没有也不会接触到TubeSDK的Producer类，考虑到业务自己搭建集群使用TubeMQ进行使用的场景，这里提供对应的使用demo，见包com.tencent.tubemq.example.MessageProducerExample类文件供参考，**需要注意**的是，业务除非使用数据平台的TubeMQ集群做MQ服务，否则仍要按照现网的接入流程使用代理层来进行数据生产：

- **i. 初始化MessageProducerExample类：**

和Consumer的初始化类似，也是构造了一个封装类，定义了一个会话工厂，以及一个Producer类，生产端的会话工厂初始化通过TubeClientConfig类进行，如之前所介绍的，ConsumerConfig类是TubeClientConfig类的子类，虽然传入参数不同，但会话工厂是通过TubeClientConfig类完成的初始化处理：

```java
public final class MessageProducerExample {

    private static final Logger logger = 
        LoggerFactory.getLogger(MessageProducerExample.class);
    private static final ConcurrentHashMap<String, AtomicLong> counterMap = 
        new ConcurrentHashMap<String, AtomicLong>();
    String[] arrayKey = {"aaa", "bbb", "ac", "dd", "eee", "fff", "gggg", "hhhh"};
    private MessageProducer messageProducer;
    private TreeSet<String> filters = new TreeSet<String>();
    private int keyCount = 0;
    private int sentCount = 0;
    private MessageSessionFactory messageSessionFactory;

    public MessageProducerExample(final String localHost, final String masterHostAndPort) 
        throws Exception {
        filters.add("aaa");
        filters.add("bbb");
        TubeClientConfig clientConfig = 
            new TubeClientConfig(localHost, masterHostAndPort);
        this.messageSessionFactory = new TubeSingleSessionFactory(clientConfig);
        this.messageProducer = this.messageSessionFactory.createProducer();
    }
}
```



- **ii. 发布Topic：**

```java
public void publishTopics(List<String> topicList) throws TubeClientException {
    this.messageProducer.publish(new TreeSet<String>(topicList));
}
```



- **iii. 进行数据生产：**

如下所示，则为具体的数据构造和发送逻辑，构造一个Message对象后调用sendMessage()函数发送即可，有同步接口和异步接口选择，依照业务要求选择不同接口；需要注意的是该业务根据不同消息调用message.putSystemHeader()函数设置消息的过滤属性和发送时间，便于系统进行消息过滤消费，以及指标统计用。完成这些，一条消息即被发送出去，如果返回结果为成功，则消息被成功的接纳并且进行消息处理，如果返回失败，则业务根据具体错误码及错误提示进行判断处理，相关错误详情见《TubeMQ错误信息介绍.xlsx》：

```java
public void sendMessageAsync(int id, long currtime,
                             String topic, byte[] body,
                             MessageSentCallback callback) {
    Message message = new Message(topic, body);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
    long currTimeMillis = System.currentTimeMillis();
    message.setAttrKeyVal("index", String.valueOf(1));
    String keyCode = arrayKey[sentCount++ % arrayKey.length];
    message.putSystemHeader(keyCode, sdf.format(new Date(currTimeMillis))); 
    if (filters.contains(keyCode)) {
        keyCount++;
    }
    try {
        message.setAttrKeyVal("dataTime", String.valueOf(currTimeMillis));
        messageProducer.sendMessage(message, callback);
    } catch (TubeClientException e) {
        logger.error("Send message failed!", e);
    } catch (InterruptedException e) {
        logger.error("Send message failed!", e);
    }
}
```



- **iv. Producer不同类MAMessageProducerExample关注点：**

该类初始化与MessageProducerExample类不同，采用的是TubeMultiSessionFactory多会话工厂类进行的连接初始化，该demo提供了如何使用多会话工厂类的特性，可以用于通过多个物理连接提升系统吞吐量的场景（TubeMQ通过连接复用模式来减少物理连接资源的使用），恰当使用可以提升系统的生产性能。在Consumer侧也可以通过多会话工厂进行初始化，但考虑到消费是长时间过程处理，对连接资源的占用比较小，消费场景不推荐使用。

 

自此，整个生产和消费的示例已经介绍完，大家可以直接下载对应的代码编译跑一边，看看是不是就是这么简单😊

---
<a href="#top">Back to top</a>
 

 

 

 