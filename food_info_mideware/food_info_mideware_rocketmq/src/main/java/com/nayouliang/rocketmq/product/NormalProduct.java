package com.nayouliang.rocketmq.product;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
/**
 * 一般情况下的消息发送
 * @author sly
 *
 */
public class NormalProduct extends Product {
	public NormalProduct(String groupName) throws MQClientException  {
		super(groupName,1);
	
	}

	public SendResult sendMessage(String message,String topic,String tag) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		if(tag == null) {
			tag="*";
		}
		Message msg = new Message(topic,// topic  
              tag,// tag  
              message.getBytes()// body  
      );
		return super.producer.send(msg);  
	}

	@Override
	public SendResult sendMessage(String message, String topic, String tag,
			LocalTransactionExecuter localTransactionExecuter)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		return null;
	}

}
