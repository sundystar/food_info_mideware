package com.nayouliang.rocketmq.product.strategy;

import com.nayouliang.rocjetmq.Product;

public class ProductStrategy {

	private Product product;
	
	private ProductStrategy(Product product) {
		this.product = product;
	}
	
	public void exec(String message) {
		product.SendMessage(message);
	}
	
}
