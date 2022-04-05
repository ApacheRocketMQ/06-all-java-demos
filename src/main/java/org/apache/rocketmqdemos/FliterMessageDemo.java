package org.apache.rocketmqdemos;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 1. tag过滤
 */
public class FliterMessageDemo {
    public static String NAMESRV_ADDR = "127.0.0.1:9876";
    public static String TOPIC = "tiger_topic_03";

    public static void main(String args[]) throws Exception {
        if (args != null && args.length > 0) {
            NAMESRV_ADDR = args[0];
        }

        if (args != null && args.length > 1 && "tag".equals(args[1])) {
            mockTagFilter();
            return;
        }

        if (args != null && args.length > 1 && "sql".equals(args[1])) {
            mockSQLFilter();
            return;
        }

        System.out.println("正确命令格式: \njava -jar xxx.jar 1.1.1.1:9876 tag \njava -jar xxx.jar 1.1.1.1:9876 sql");
    }

    private static void mockSQLFilter() throws Exception {
        startProducer();

        System.out.println("启动按照属性过滤的消费者");
        consume1("tom", "tiger-consumer-sql-01");
    }


    public static void mockTagFilter() throws Exception {
        startProducer();

        String tag = "order_create";
        System.out.println("启动消费tag=" + tag + "的消费者");
        consume(tag, "tiger-consumer-tag1");

        tag = "*";
        System.out.println("启动消费全部tag的消费者");
        consume(tag, "tiger-consumer-tag-all");

        Thread.sleep(15000);
    }

    public static String userKey = "user";

    public static void startProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("tiger-producer-03");
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();

        Message msg = new Message(TOPIC, ("Hello RocketMQ, Create Order").getBytes(RemotingHelper.DEFAULT_CHARSET));
        msg.setTags("order_create");
        msg.putUserProperty(userKey, "jim");
        producer.send(msg);
        System.out.println(userKey + "=" + msg.getProperty(userKey) + " 的消息已经发送");

        msg.setBody("Hello RocketMQ, Done Order".getBytes(StandardCharsets.UTF_8));
        msg.setTags("order_done");
        msg.putUserProperty("user", "tom");
        producer.send(msg);
        System.out.println(userKey + "=" + msg.getProperty(userKey) + " 的消息已经发送");

        producer.shutdown();
    }

    public static void consume(String tags, String consumerGroupName) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroupName);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.subscribe(TOPIC, tags);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("[指定消费tag=%s] %s \n", tags, new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }


    public static void consume1(String user, String consumerGroupName) throws Exception {
        String SQL = userKey + " = '" + user + "'";

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroupName);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.subscribe(TOPIC, MessageSelector.bySql(SQL));
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("[指定消费属性key user=%s] 消息属性user=%s, 消息体=%s \n", user, msg.getProperty("user"), new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }
}
