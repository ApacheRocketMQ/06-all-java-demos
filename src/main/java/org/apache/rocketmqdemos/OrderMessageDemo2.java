package org.apache.rocketmqdemos;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

import java.util.Collections;
import java.util.List;
import java.util.Set;

//全局顺序消息demo
public class OrderMessageDemo2 {
    public static String NAMESRV_ADDR = "127.0.0.1:9876";
    public static String TOPIC_1_QUEUE = "tiger_order_topic_1_queue";

    public static void main(String args[]) throws Exception {
        if (args != null && args.length > 0) {
            NAMESRV_ADDR = args[0];
        }

        globalOrderMessageDemo();

        Thread.sleep(100000);
    }

    // 全局有序
    public static void globalOrderMessageDemo() throws Exception {
        createOrderlyTopic(1, TOPIC_1_QUEUE);

        consume("*", "tiger-consumer-orderly_2_queue");

        startProducer();

        Thread.sleep(15000);
    }

    public static void startProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("tiger-producer-04");
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();

        for (int i = 1; i <= 10; i++) {
            Message msg = new Message(TOPIC_1_QUEUE, ("Hello RocketMQ, " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // key计算的hash相同的消息， 会放在一个queue中
            msg.setKeys(String.valueOf(i % 3));
            producer.send(msg);
        }

        producer.shutdown();
    }

    public static void consume(String tags, String consumerGroupName) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroupName);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.subscribe(TOPIC_1_QUEUE, tags);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.registerMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("[顺序消息消费] queueId=%d, body=%s \n", msg.getQueueId(), new String(msg.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        System.out.println("消费者启动成功");
    }

    public static void createOrderlyTopic(int queueNumber, String topicName) {
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, NAMESRV_ADDR);

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setReadQueueNums(queueNumber);
            topicConfig.setWriteQueueNums(queueNumber);
            topicConfig.setTopicName(topicName);

            defaultMQAdminExt.start();

            String clusterName = "";
            for (String cn : defaultMQAdminExt.examineBrokerClusterInfo().getClusterAddrTable().keySet()) {
                clusterName = cn;
                break;
            }
            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);

            defaultMQAdminExt.deleteTopicInBroker(masterSet, topicName);
            defaultMQAdminExt.deleteTopicInNameServer(Collections.singleton(NAMESRV_ADDR), topicName);

            for (String addr : masterSet) {
                defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
                System.out.printf("create topic to %s success.%n", addr);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }

}
