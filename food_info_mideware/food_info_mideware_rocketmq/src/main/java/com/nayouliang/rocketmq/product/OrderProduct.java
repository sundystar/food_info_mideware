package com.nayouliang.rocketmq.product;

import java.util.List;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
/**
 * 顺序消息传递
 * @author sly
 *
 */
public class OrderProduct extends Product {

	public OrderProduct(String groupName) throws MQClientException {
		super(groupName,1);
	}

	@Override
	public SendResult sendMessage(String message, String topic, String tag)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException { 
      	  
          Message msg = new Message(topic, tag, message.getBytes());  

          SendResult sendResult = super.producer.send(msg, new MessageQueueSelector() {  
              public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {  
                  Integer id = (Integer) arg;  
                  int index = id % mqs.size();  
                  return mqs.get(index);  
              }  
          }, 0);  
          return sendResult;
	}

	@Override
	public SendResult sendMessage(String message, String topic, String tag,
			LocalTransactionExecuter localTransactionExecuter)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		return null;
	}

	

}
