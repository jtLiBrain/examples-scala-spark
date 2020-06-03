# 描述一下Kafka各类操作如何使用Zookeeper
- `>`：表示Zookeeper命令
- `#`：表示Linux命令行

## 1. 启动Zookeeper
```
# bin/zookeeper-server-start.sh config/zookeeper.properties
```
zookeeper中节点状态如下：
```
> ls /
[zookeeper]

> ls /zookeeper
[quota]

> ls /zookeeper/quota
[]
```

## 2. 启动Kafka
```
# bin/kafka-server-start.sh config/server.properties
```
zookeeper中节点状态如下：
```
> ls /
[cluster, controller_epoch, controller, brokers, zookeeper, admin, isr_change_notification, consumers, log_dir_event_notification, latest_producer_id_block, config]
```

## 3. 创建topic
创建：
```
# bin/kafka-topics.sh --create --bootstrap-server 192.168.56.101:9092 --replication-factor 1 --partitions 2 --topic test
```

查看：
```
# bin/kafka-topics.sh --list --bootstrap-server 192.168.56.101:9092
test
```

查看主题详细：
```
# bin/kafka-topics.sh --describe --bootstrap-server 192.168.56.101:9092 --topic test
Topic:test    PartitionCount:2    ReplicationFactor:1    Configs:segment.bytes=1073741824
    Topic: test    Partition: 0    Leader: 0    Replicas: 0    Isr: 0
    Topic: test    Partition: 1    Leader: 0    Replicas: 0    Isr: 0
```

zookeeper中节点状态如下：
```
> ls /brokers/topics
[test]

> ls /brokers/topics/test/partitions
[0, 1]

> ls /config/topics
[test]
```
## 3. 工具
1. https://github.com/DeemOpen/zkui