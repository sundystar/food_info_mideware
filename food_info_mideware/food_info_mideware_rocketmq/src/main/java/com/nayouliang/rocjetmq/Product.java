package com.nayouliang.rocjetmq;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.foodinfo.base.SysConfig;
import com.foodinfo.enums.SysConfigEnum;

public class Product {

	private static DefaultMQProducer producer = null;  
      

    public static void init() throws MQClientException  {
    	try {
    		producer = new DefaultMQProducer("please_rename_unique_group_name");  
    		producer.setNamesrvAddr(SysConfig.getConfig("namesrvAddr", SysConfigEnum.ROCKETMQ));  
            producer.setRetryTimesWhenSendFailed(10);//失败的 情况发送10次  
            producer.start();
    	}catch(MQClientException e) {
    		throw e;
    	}
    	
    }
    
    
    public static SendResult SendMessage(String object) {
    	try {
			if(producer == null) {
	    		synchronized (producer) {
	    			init();
	    		}
        	}
			
			
			
			
			
        	return null;
    	}catch(Exception e) {
    		e.printStackTrace();
    	}
    	return null;

    }
	
}
