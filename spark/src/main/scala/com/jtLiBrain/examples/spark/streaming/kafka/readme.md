StreamingExample2
设置：enable.auto.commit=true，
结论：1. 会周期性的向kafka的 __consumer_offsets 这个topic提交offset，这时可以观察到这个topic会不断有消息周期性被写进来
     2. 当重启流式作业时，只要 groupId 不变，那么即使我们下面的代码没有为KafkaUtils.createDirectStream设置起始的offset，那么流式作业
     也不会重复消费之前的已经消费过的消息了
     '''
     val kafkaInputDStream = KafkaUtils.createDirectStream[String, String] (
             streamingContext,
             LocationStrategies.PreferConsistent,
             ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
           )
    '''
    3. 对于 KafkaUtils.createDirectStream ，因为在实践当中会发生kafka的数据丢失的情况，到时__consumer_offsets记录的
     offset所对应的数据可能已经在kafka中丢失了（数据丢失、数据到期被kafka清理掉），这时可能就需要我们自己来重新修正offset，
     将其设置给KafkaUtils.createDirectStream

StreamingExample3
设置：enable.auto.commit=false，但我们每个batch完成后，手动向kafka提交了offset
结论：1. 会在每个batch完成后向kafka的 __consumer_offsets 这个topic提交offset，这时可以观察到这个topic会不断有消息周期性被写进来
     2. 当重启流式作业时，只要 groupId 不变，那么即使我们下面的代码没有为KafkaUtils.createDirectStream设置起始的offset，那么流式作业
     也不会重复消费之前的已经消费过的消息了
     '''
     val kafkaInputDStream = KafkaUtils.createDirectStream[String, String] (
             streamingContext,
             LocationStrategies.PreferConsistent,
             ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
           )
    '''
    3. 对于 KafkaUtils.createDirectStream ，因为在实践当中会发生kafka的数据丢失的情况，到时__consumer_offsets记录的
     offset所对应的数据可能已经在kafka中丢失了（数据丢失、数据到期被kafka清理掉），这时可能就需要我们自己来重新修正offset，
     将其设置给KafkaUtils.createDirectStream