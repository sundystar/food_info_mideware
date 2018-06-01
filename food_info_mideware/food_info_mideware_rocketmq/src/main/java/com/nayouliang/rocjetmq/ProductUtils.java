package com.nayouliang.rocjetmq;

import java.util.List;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;  
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.foodinfo.base.SysConfig;
import com.foodinfo.enums.SysConfigEnum;  

/** 
 * Producer，发送消息 
 *  
 */  
public class ProductUtils {  
    public static void main(String[] args) throws MQClientException, InterruptedException, RemotingException, MQBrokerException {  
        /**
         * 事务支持类
         */
        TransactionCheckListener transactionCheckListener = new TransactionCheckListenerImpl();  
        TransactionMQProducer producer = new TransactionMQProducer("transaction_Producer");  
        // 事务回查最小并发数  
        producer.setCheckThreadPoolMinSize(2);  
        // 事务回查最大并发数  
        producer.setCheckThreadPoolMaxSize(2);  
        // 队列数  
        producer.setCheckRequestHoldMax(2000);  
        producer.setTransactionCheckListener(transactionCheckListener); 
        
        
        
        
//    	DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");  
        producer.setNamesrvAddr(SysConfig.getConfig("namesrvAddr", SysConfigEnum.ROCKETMQ));  
//        producer.setRetryTimesWhenSendFailed(10);//失败的 情况发送10次  
        producer.start();  
  
//        for (int i = 0; i < 1000; i++) {  
//            try {  
//                Message msg = new Message("TopicTest",// topic  
//                        "TagA",// tag  
//                        ("Hello RocketMQ " + i).getBytes()// body  
//                );  
//                SendResult sendResult = producer.send(msg);  
//                System.out.println(sendResult);  
//            } catch (Exception e) {  
//                e.printStackTrace();  
//                Thread.sleep(1000);  
//            }  
//        } 
        
        /**
         * 多生产方，消费也必须按照顺序消费
         */
//        for (int i = 1; i <= 5; i++) {  
//        	  
//            Message msg = new Message("TopicOrderTest", "order_1", "KEY" + i, ("order_1 " + i).getBytes());  
//
//            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {  
//                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {  
//                    Integer id = (Integer) arg;  
//                    int index = id % mqs.size();  
//                    return mqs.get(index);  
//                }  
//            }, 0);  
//
//            System.err.println(sendResult);  
//        }  
//        
//        for (int i = 1; i <= 5; i++) {  
//        	  
//            Message msg = new Message("TopicOrderTest", "order_2", "KEY" + i, ("order_2 " + i).getBytes());  
//
//            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {  
//                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {  
//                    Integer id = (Integer) arg;  
//                    int index = id % mqs.size();  
//                    return mqs.get(index);  
//                }  
//            }, 1);  
//
//            System.err.println(sendResult);  
//        }
//        
//        
//        for (int i = 1; i <= 5; i++) {  
//        	  
//            Message msg = new Message("TopicOrderTest", "order_3", "KEY" + i, ("order_3 " + i).getBytes());  
//
//            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {  
//                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {  
//                    Integer id = (Integer) arg;  
//                    int index = id % mqs.size();  
//                    return mqs.get(index);  
//                }  
//            }, 2);  
//        
//            System.err.println(sendResult);  
//        } 
        
        /**
         * 处理事务
         */
        
        TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl(); 
        for (int i = 1; i <= 2; i++) {  
            try {  
                Message msg = new Message("TopicTransactionTest2", "transaction" + i, "KEY" + i,  
                        ("Hello RocketMQ " + i).getBytes());  
                SendResult sendResult = producer.sendMessageInTransaction(msg, tranExecuter, null);  
                System.out.println(sendResult);  
  
                Thread.sleep(10);  
            } catch (MQClientException e) {  
                e.printStackTrace();  
            }  
        }  
  
        for (int i = 0; i < 100000; i++) {  
            Thread.sleep(1000);  
        }  
        
        producer.shutdown();  
    }  
}  