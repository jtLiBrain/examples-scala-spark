# Kafka消费者API如何使用启动Zookeeper
- `>`：表示Zookeeper命令
- `#`：表示Linux命令行

## 1. 准备数据：向`test`主题中添加9条数据
```shell
# bin/kafka-console-producer.sh --broker-list 192.168.56.101:9092 --topic test
>1
>2
>3
>4
>5
>6
>7
>8
>9
>
```

## 2. 启动消费者
```
@Before
public void before() {
    Properties props = new Properties();

    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group1");
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 为了方便测试，我们关闭自动提交offset的功能，使用程序进行手动提交
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "3"); // 每个调用poll返回的最大条数
    props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1800000"); // 为了方便测试，我们将该值设置的足够大
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(topic));
}

@After
public void after() {
    if(consumer != null)
        consumer.close();
}

@Test
public void testPoll() {
    int recordCounter = 0;

    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            recordCounter++;

            System.out.printf(
                    "topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
        }

        System.out.println("");
        consumer.commitSync();
        System.out.println("");

        if(recordCounter == 9) {
            return;
        }
    }
}
```
输出：
>  （第1次轮询）  
> topic = test, partition = 1, offset = 0, key = null, value = 1  
> topic = test, partition = 1, offset = 1, key = null, value = 3  
> topic = test, partition = 1, offset = 2, key = null, value = 5  
>   
>  （第2次轮询）  
> topic = test, partition = 1, offset = 3, key = null, value = 7  
> topic = test, partition = 1, offset = 4, key = null, value = 9  
> topic = test, partition = 0, offset = 0, key = null, value = 2  
> 
>  （第3次轮询）  
> topic = test, partition = 0, offset = 1, key = null, value = 4  
> topic = test, partition = 0, offset = 2, key = null, value = 6  
> topic = test, partition = 0, offset = 3, key = null, value = 8  


## 3. 启动消费者，后kafka会创建`__consumer_offsets`主题
zookeeper中节点状态如下：
```
> ls /brokers/topics
[test, __consumer_offsets]
```

## 4. 监控`__consumer_offsets`这个topic
```
# bin/kafka-console-consumer.sh --bootstrap-server 192.168.56.101:9092 --from-beginning --topic __consumer_offsets

onsumeÿÿÿÿrzÿd                                                                                                                                                                                                              consumerrange/consumer-1-1b157954-c749-49ed-8064-5985def57ad4rz:/consumer-1-1b157954-c749-49ed-8064-5985def57ad4ÿÿ
/192.168.56.1󾾐testtest 
Xshell

（调用每次consumer.commitSync();都会输出下面结果，总计输出三次）
ÿÿÿÿrzdp
ÿÿÿÿrzdp


ÿÿÿÿrzf
ÿÿÿÿrzf
Xshell


ÿÿÿÿrz 
ÿÿÿÿrz 
Xshell

（调用consumer.close();会输出下面结果）
consumerÿÿÿÿrz
```

 ## 5. 再次启动消费者程序
 在消费完9条记录后，程序退出；再次以`test-group1`启动程序，不会消费出新的记录，因为`test-group1`这个消费组的offset记录在`__consumer_offsets`这个topic中已经被记载了