package org.apache.rocketmqdemos;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

//延迟消息demo
public class DelayMessageDemo {
    public static String NAMESRV_ADDR = "127.0.0.1:9876";
    public static String ACCESS_KEY = "rocketmq2";
    public static String SECRET_KEY = "12345678";
    public static String TOPIC = "tiger_topic_03";


    public static long sendTimeMs = 0l;

    public static void main(String args[]) throws Exception {
        if (args != null && args.length > 0) {
            NAMESRV_ADDR = args[0];
        }
        startProducer();
        pushConsumer();

        Thread.sleep(15000);
    }

    public static void startProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("tiger-producer-03");
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();


        int level = 1; // 延迟1s
        Message msg = new Message(TOPIC, ("Hello RocketMQ, level=" + level).getBytes(RemotingHelper.DEFAULT_CHARSET));
        msg.setDelayTimeLevel(level);
        SendResult sendResult = producer.send(msg);
        sendTimeMs = System.currentTimeMillis();
        System.out.printf("[发送成功level=%d], id = %s%n", level, sendResult.getMsgId());


        level = 2; // 延迟5s
        msg = new Message(TOPIC, ("Hello RocketMQ, level=" + level).getBytes(RemotingHelper.DEFAULT_CHARSET));
        msg.setDelayTimeLevel(level);
        sendResult = producer.send(msg);
        sendTimeMs = System.currentTimeMillis();
        System.out.printf("[发送成功level=%d], id = %s%n", level, sendResult.getMsgId());

        level = 3;//延迟10s
        msg = new Message(TOPIC, ("Hello RocketMQ, level=" + level).getBytes(RemotingHelper.DEFAULT_CHARSET));
        msg.setDelayTimeLevel(level);
        sendResult = producer.send(msg);
        sendTimeMs = System.currentTimeMillis();
        System.out.printf("[发送成功level=%d], id = %s%n", level, sendResult.getMsgId());


        // 不延迟
        msg = new Message(TOPIC, ("Hello RocketMQ, 不延迟").getBytes(RemotingHelper.DEFAULT_CHARSET));
        sendResult = producer.send(msg);
        sendTimeMs = System.currentTimeMillis();
        System.out.printf("[发送成功level=%d], id = %s%n", level, sendResult.getMsgId());

        producer.shutdown();
    }

    public static void pushConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tiger_consumer_03");
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.subscribe(TOPIC, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    long now = System.currentTimeMillis();
                    System.out.printf("[消费消息] %s, 消费时间: %d, 与发送相差毫秒=%d\n", new String(msg.getBody()) , now, now - sendTimeMs);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }


}
