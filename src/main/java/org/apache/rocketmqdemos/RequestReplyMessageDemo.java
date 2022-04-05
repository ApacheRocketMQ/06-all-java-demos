package org.apache.rocketmqdemos;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.utils.MessageUtil;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

// request reply消息demo
public class RequestReplyMessageDemo {
    public static String NAMESRV_ADDR = "127.0.0.1:9876";
    public static String TOPIC = "tiger_request_topic_01";
    public static String PRODUCER_GROUP_NAME = "tiger_producer_group_request";

    public static void main(String args[]) throws MQClientException, InterruptedException {
        if (args != null && args.length > 0) {
            NAMESRV_ADDR = args[0];
        }

        new Thread(() -> {
            try {
                responseConsumer();
            } catch (MQClientException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        requestProeucer();
    }

    public static void requestProeucer() throws MQClientException {

        long ttl = 10000;

        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP_NAME);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();

        try {
            Message msg = new Message(TOPIC, "这个是request消息， 快点回复我".getBytes(RemotingHelper.DEFAULT_CHARSET));
            long begin = System.currentTimeMillis();

            Message retMsg = producer.request(msg, ttl);
            
            long cost = System.currentTimeMillis() - begin;
            System.out.printf("\n发送消息到回复花费: %d ms,  回复内容: %s %n", cost, new String(retMsg.getBody()));
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.shutdown();
    }

    public static void responseConsumer() throws MQClientException, InterruptedException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tiger-consumer-group_05");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.subscribe(TOPIC, "*");
        consumer.setPullTimeDelayMillsWhenException(0L);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        System.out.printf("[消费消息]已经收到消息，内容： %s\n\n", new String(msg.getBody()));

                        String replyTo = MessageUtil.getReplyToClient(msg);
                        byte[] replyContent = "我收到了la".getBytes();
                        Message replyMessage = MessageUtil.createReplyMessage(msg, replyContent);

                        DefaultMQProducer replyProducer = new DefaultMQProducer("tiger_producer_group_reply_01");
                        replyProducer.setNamesrvAddr(NAMESRV_ADDR);
                        replyProducer.start();

                        SendResult replyResult = replyProducer.send(replyMessage, 10000);

                        System.out.printf("reply to %s , %s %n", replyTo, replyResult.toString());
                        replyProducer.shutdown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });


        consumer.start();

        Thread.sleep(999999);
    }
}
