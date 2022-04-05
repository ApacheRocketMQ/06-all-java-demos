package org.apache.rocketmqdemos;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// 事物消息demo
public class TransactionMessageDemo {
    public static String TOPIC = "topic_001";
    public static String NAME_SRV_ADDR = "127.0.0.1:9876";
    public static String CONSUMER_GROUP = "consumer_001";

    public static void main(String[] args) throws Exception {
        if (args != null && args.length > 0) {
            NAME_SRV_ADDR = args[0];
        }
        if (args != null && args.length > 1) {
            TOPIC = args[1];
        }
        if (args != null && args.length > 2) {
            CONSUMER_GROUP = args[2];
        }

        sendMessages();

    }


    /**
     * 初始化生产者， 发送顺序消息
     */
    public static void sendMessages() throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("producer_group");
        producer.setNamesrvAddr(NAME_SRV_ADDR);
        producer.setExecutorService(new ThreadPoolExecutor(1, 1, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        }));
        producer.setTransactionListener(new TransactionListenerImpl());
        producer.start();

        //发送直接可以被消费的消息
        Message msg1 = new Message(TOPIC, ("0. 本地事物处理状态未知，通过回查发现本地事物成功，被消费者消费").getBytes(RemotingHelper.DEFAULT_CHARSET));
        producer.sendMessageInTransaction(msg1, 0);
        System.out.println("发送第1个half消息完成");


        //发送需要回查的消息
        Message msg = new Message(TOPIC, ("1. 本地事物成功， 不用回查， 被消费者消费").getBytes(RemotingHelper.DEFAULT_CHARSET));
        producer.sendMessageInTransaction(msg, 1);
        System.out.println("发送第2个half消息完成");


        //发送本地事物执行失败， 需要回滚的消息。 这个消息消费者永远看不到
        Message msg2 = new Message(TOPIC, ("2. 本地事物失败，回滚half消息， 消费者永远不会消费").getBytes(RemotingHelper.DEFAULT_CHARSET));
        producer.sendMessageInTransaction(msg2, 2);
        System.out.printf("发送第3个half消息完成", new String(msg2.getBody()));

        consume();

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }
        producer.shutdown();
    }

    public static void consume() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAME_SRV_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(TOPIC, "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("[消费] body=%s%n", new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("\n\n消费者启动成功");
    }

    public static class TransactionListenerImpl implements TransactionListener {
        @Override
        public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
            System.out.printf("[处理本地事物] %s%n%n", new String(msg.getBody()));
            int flag = Integer.parseInt(arg.toString());
            switch (flag) {
                case 0:
                    return LocalTransactionState.UNKNOW; // 本地事物状态未知， 待回查
                case 1:
                    return LocalTransactionState.COMMIT_MESSAGE; // 本地事物执行成功， 消费者可以消费这个消息
                case 2:
                    return LocalTransactionState.ROLLBACK_MESSAGE; // 消费者不会消费这个消息
                default:
                    return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        }

        // 消息内容是 "Hello RocketMQ, Check Producer" 的消息会被回查。 然后被消费
        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt msg) {
            System.out.printf("[回查本地事物] 消息体：%s %n%n", new String(msg.getBody()));
            return LocalTransactionState.COMMIT_MESSAGE;
        }
    }
}
