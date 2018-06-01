package com.nayouliang.rocketmq.product;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.foodinfo.base.SysConfig;
import com.foodinfo.enums.SysConfigEnum;


public abstract class Product {

   
	DefaultMQProducer producer =null;   

	public Product(String groupName,int mark) throws MQClientException {
		if(1==mark) {
			new DefaultMQProducer(groupName);
			producer.setNamesrvAddr(SysConfig.getConfig("namesrvAddr", SysConfigEnum.ROCKETMQ));  
	        producer.setRetryTimesWhenSendFailed(10);//失败的 情况发送10次  
	        producer.start();
		}
	}
	
	public abstract SendResult sendMessage(String message,String topic,String tag) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
	public abstract SendResult sendMessage(String message,String topic,String tag,LocalTransactionExecuter localTransactionExecuter) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
	
}
