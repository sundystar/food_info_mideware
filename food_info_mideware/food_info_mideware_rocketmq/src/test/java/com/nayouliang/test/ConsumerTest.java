package com.nayouliang.test;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.nayouliang.rocjetmq.ConsumerUtils;

public class ConsumerTest {

	public static void main(String[] args) throws InterruptedException, MQClientException {
		ConsumerUtils.getInstance().receiveMessage6();
	}
	
}
