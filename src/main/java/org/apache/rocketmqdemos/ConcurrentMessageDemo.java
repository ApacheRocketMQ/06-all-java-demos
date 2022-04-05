package org.apache.rocketmqdemos;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * 并发消息的Demo
 */
public class ConcurrentMessageDemo {
    public static final String TOPIC_NAME = "tiger_topic_01";
    public static String NAMESRV_ADDRESSES = "127.0.0.1:9876";
    public static DefaultMQProducer producer = new DefaultMQProducer("tiger-producer");

    public static void main(String[] args) throws InterruptedException, MQClientException {
        if (args != null && args.length > 0) {
            NAMESRV_ADDRESSES = args[0];
        }
        producer.setNamesrvAddr(NAMESRV_ADDRESSES);
        producer.start();

        startOneConsumer();
        
        for (int i = 1; i <= 10; i++) { // 模拟并发发送消息
            int finalI = i;
            new Thread(() -> {
                try {
                    startOneProducer(finalI);
                } catch (Exception e) {
                    System.out.printf("发送失败" + e.getMessage());
                    e.printStackTrace();
                }
            }).start();
        }

        Thread.sleep(9999999);
    }

    // 启动一个生产者实例， 并且生产10条消息
    public static void startOneProducer(int producerIndex) throws InterruptedException, MQClientException, MQBrokerException, RemotingException, UnsupportedEncodingException {
        Message msg = new Message(TOPIC_NAME, ("Hello RocketMQ by producer " + producerIndex).getBytes(RemotingHelper.DEFAULT_CHARSET));
        SendResult res = producer.send(msg);
        System.out.printf("生产者编号：%d 消息发送成功, 消息id=%s\n", producerIndex, res.getMsgId());
    }

    public static void startOneConsumer() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tiger_consumer_01");
        consumer.setNamesrvAddr(NAMESRV_ADDRESSES);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(TOPIC_NAME, "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("[正在消费消息] %s = %s\n", msg.getMsgId(), new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.\n");
    }


}
