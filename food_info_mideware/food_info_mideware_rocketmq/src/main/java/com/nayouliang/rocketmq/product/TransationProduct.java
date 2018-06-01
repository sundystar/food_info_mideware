package com.nayouliang.rocketmq.product;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
/**
 * 严格的消息传递，事务管理
 * @author sly
 *
 */
public class TransationProduct extends Product {
    
	TransactionMQProducer producer=null;

	public TransationProduct(String groupName) throws MQClientException {
		super(groupName,2);

		producer = new TransactionMQProducer(groupName);  
	    // 事务回查最小并发数  
	    producer.setCheckThreadPoolMinSize(2);  
	    // 事务 回查最大并发数  
	    producer.setCheckThreadPoolMaxSize(2);  
	    // 队列数  
	    producer.setCheckRequestHoldMax(2000);  
	    
	    producer.start();
	}

	@Override
	public SendResult sendMessage(String message, String topic, String tag,LocalTransactionExecuter tranExecuter)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
            try {  
                Message msg = new Message(topic, topic, tag,  
                		message.getBytes());  
                SendResult sendResult = producer.sendMessageInTransaction(msg, tranExecuter, null);  
                return sendResult;
            } catch (MQClientException e) {  
                e.printStackTrace();  
            }  	    
	        return null;
	}

	@Override
	public SendResult sendMessage(String message, String topic, String tag)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		return null;
	}

	

}
