package org.apache.rocketmqdemos;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * ACL 支持Demo
 */
public class ACLDemo {
    public static String NAMESRV_ADDR = "127.0.0.1:9876";
    public static String ACCESS_KEY = "rocketmq2";
    public static String SECRET_KEY = "12345678";
    public static String TOPIC = "tiger_topic_02";

    public static void main(String args[]) throws Exception {
        if (args != null && args.length > 0) {
            NAMESRV_ADDR = args[0];
        }
        startProducer();
        pushConsumer();

        Thread.sleep(1000000);
    }

    public static void startProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("tiger-producer", getAclRPCHook());
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();

        Message msg = new Message(TOPIC, "Hello RocketMQ".getBytes(RemotingHelper.DEFAULT_CHARSET));
        SendResult sendResult = producer.send(msg);
        System.out.printf("%s%n", sendResult);
        producer.shutdown();
    }

    public static void pushConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tiger_consumer_01", getAclRPCHook(), new AllocateMessageQueueAveragely());
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.subscribe(TOPIC, "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("[消费消息, id=%s, body=%s]%n", msg.getMsgId(), new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

    static RPCHook getAclRPCHook() {
        return new AclClientRPCHook(new SessionCredentials(ACCESS_KEY, SECRET_KEY));
    }
}
