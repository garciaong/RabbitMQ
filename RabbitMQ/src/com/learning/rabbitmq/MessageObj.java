package com.learning.rabbitmq;

import java.io.Serializable;

public class MessageObj implements Serializable{

	public String type;
	public String message;
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	
}
