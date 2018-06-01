package com.nayouliang.rocketmq.consumer;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;  
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;  
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;  
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;  
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.foodinfo.base.SysConfig;
import com.foodinfo.enums.SysConfigEnum;  
  
/** 
 * Consumer，订阅消息 
 */  
public class ConsumerUtils {  
  
	private static ConsumerUtils instance =null;
	
    private DefaultMQPushConsumer consumer = null;
	
	private ConsumerUtils(String groupName) {
		consumer = new DefaultMQPushConsumer(UUID.randomUUID().toString().replaceAll("-", ""));  
		String addr = SysConfig.getConfig("namesrvAddr",SysConfigEnum.ROCKETMQ);
		consumer.setNamesrvAddr(addr);  
        consumer.setConsumeMessageBatchMaxSize(10);  
        /** 
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br> 
         * 如果非第一次启动，那么按照上次消费的位置继续消费 
         */  
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);  
	}
	
	public static synchronized ConsumerUtils getInstance(String groupName) {
		if(instance == null ) {
			synchronized (instance) {
				return new ConsumerUtils(groupName);
			}
		}
		return instance;
	}
	/**
	 * 接受消息
	 * @return
	 * @throws InterruptedException
	 * @throws MQClientException
	 */
    public boolean receiveMessage() throws InterruptedException, MQClientException {  
        
  
        consumer.subscribe("TopicTest", "*");  
  
        consumer.registerMessageListener(new MessageListenerConcurrently() {  
  
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {  
                  
                try {  
                	// 表示业务处理时间    
  
                    for (MessageExt msg : msgs) {  
                        System.out.println(" Receive New Messages: " + new Date(msg.getStoreTimestamp()));  
                    }
                    
                } catch (Exception e) {  
                    e.printStackTrace();  
                          
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;// 重试  
                    
                }  
               
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;  
            }  
        });  
  
        consumer.start();  
  
        System.out.println("Consumer Started.");
		return false;  
    }  
    
    /**
     * 重复接受
     * @return
     * @throws InterruptedException
     * @throws MQClientException
     */
    public boolean receiveMessage2() throws InterruptedException, MQClientException {  
        
    	  
        consumer.subscribe("TopicTest", "*");  
  
        consumer.registerMessageListener(new MessageListenerConcurrently() {  
  
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {  
                  
                try {  
                	// 表示业务处理时间    
  
                    for (MessageExt msg : msgs) {  
                        System.out.println(" Receive New Messages: " + new Date(msg.getStoreTimestamp()));  
                    }  
                } catch (Exception e) {  
                    e.printStackTrace();  
                    if(msgs.get(0).getReconsumeTimes()==3){  
                        //记录日志  
                          
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;// 成功  
                    }else{  
                          
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;// 重试  
                    }
                }  
               
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;  
            }  
        });  
  
        consumer.start();  
  
        System.out.println("Consumer Started.");
		return false;  
    }
    
    /**
     * 
     * 消费模式   广播消费
     * @return
     * @throws InterruptedException
     * @throws MQClientException
     */
    public boolean receiveMessage3() throws InterruptedException, MQClientException {  
        
        consumer.setMessageModel(MessageModel.BROADCASTING);// 广播消费  

        consumer.subscribe("TopicTest", "*");  
  
        consumer.registerMessageListener(new MessageListenerConcurrently() {  
  
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {  
                  
                try {  
                	// 表示业务处理时间    
  
                    for (MessageExt msg : msgs) {  
                        System.out.println(" Receive New Messages: " + new Date(msg.getStoreTimestamp()));  
                    }  
                } catch (Exception e) {  
                    e.printStackTrace();  
                    if(msgs.get(0).getReconsumeTimes()==3){  
                        //记录日志  
                          
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;// 成功  
                    }else{  
                          
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;// 重试  
                    }
                }  
               
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;  
            }  
        });  
  
        consumer.start();  
  
        System.out.println("Consumer Started.");
		return false;  
    }
    
    
    /**
     * 
     * 顺序消费
     * @return
     * @throws InterruptedException
     * @throws MQClientException
     */
    public boolean receiveMessage4() throws InterruptedException, MQClientException {  
        
//        consumer.setMessageModel(MessageModel.BROADCASTING);// 广播消费  

        consumer.subscribe("TopicOrderTest2", "*");  
  
        consumer.registerMessageListener(new MessageListenerOrderly() {  
            AtomicLong consumeTimes = new AtomicLong(0);  
  
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {  
                // 设置自动提交  
                context.setAutoCommit(true);  
                for (MessageExt msg : msgs) {  
                    System.out.println(msg + ",内容：" + new String(msg.getBody()));  
                }  
  
                try {  
                    TimeUnit.SECONDS.sleep(5L);  
                } catch (InterruptedException e) {  
  
                    e.printStackTrace();  
                }  
                ;  
  
                return ConsumeOrderlyStatus.SUCCESS;  
            }
        });  
  
        consumer.start();  
  
        System.out.println("Consumer Started.");
		return false;  
    }
    
    
    /**
     * 
     * 多生产 顺序消费
     * @return
     * @throws InterruptedException
     * @throws MQClientException
     */
    public boolean receiveMessage5() throws InterruptedException, MQClientException {  
        
//        consumer.setMessageModel(MessageModel.BROADCASTING);// 广播消费  

        consumer.subscribe("TopicOrderTest", "*");  
  
        consumer.registerMessageListener(new MessageListenerOrderly() {  
            AtomicLong consumeTimes = new AtomicLong(0);  
  
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {  
                // 设置自动提交  
                context.setAutoCommit(true);  
                for (MessageExt msg : msgs) {  
                    System.err.println(msg + ",内容：" + new String(msg.getBody()));  
                }  
  
                try {  
                    TimeUnit.SECONDS.sleep(5L);  
                } catch (InterruptedException e) {  
  
                    e.printStackTrace();  
                }  
                ;  
  
                return ConsumeOrderlyStatus.SUCCESS;  
            }
        });  
  
        consumer.start();  
  
        System.out.println("Consumer Started.");
		return false;  
    }
    
    
    
    /**
     * 
     * 事务消费
     * @return
     * @throws InterruptedException
     * @throws MQClientException
     */
    public boolean receiveMessage6() throws InterruptedException, MQClientException {  
        
//        consumer.setMessageModel(MessageModel.BROADCASTING);// 广播消费  

        consumer.subscribe("TopicTransactionTest2", "*");  
        Thread.sleep(50000);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
			
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				
				 try {  
					  int i=1/0;
	                    for (MessageExt msg : msgs) {  
	                        System.err.println("食物,内容：" + new String(msg.getBody()));  
	                    }  
	  
	                } catch (Exception e) {  
	                    e.printStackTrace();  
	  
	                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;// 重试  
	                }  
	  
	                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;// 成功 
			}
		});  
  
        consumer.start();  
  
        System.out.println("Consumer Started.");
		return false;  
    }
    
    
} 