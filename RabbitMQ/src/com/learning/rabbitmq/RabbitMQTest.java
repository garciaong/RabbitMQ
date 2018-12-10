package com.learning.rabbitmq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class RabbitMQTest {

	private final static String QUEUE_NAME = "general";
	private final static String HOST = "localhost";

	public static void main(String[] args) throws IOException, TimeoutException {
		sendMQ();
		receiveMQ();
	}

	private static void sendMQ() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST);
		try (Connection connection = factory.newConnection(); 
				Channel channel = connection.createChannel()) {
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			String message = "This is a general queue!";
			MessageObj obj = new MessageObj();
			obj.setType("Basic");
			obj.setMessage(message);
//			channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
			channel.basicPublish("", QUEUE_NAME, null, toStream(obj));
			System.out.println(" Queue Message Sent : '" + message + "'");
		}
	}
	
	private static void receiveMQ() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(HOST);
	    Connection connection = factory.newConnection();
	    Channel channel = connection.createChannel();

	    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	    System.out.println(" Waiting for messages. To exit press CTRL+C");
	    
	    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
//	        String message = new String(delivery.getBody(), "UTF-8");
	    	MessageObj obj = (MessageObj)toMsgObj(delivery.getBody());
	        System.out.println(" Queue Message Received : '" + obj.getType() + "," +obj.getMessage() + "'");
	    };
	    channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });

	}
	
	public static byte[] toStream(MessageObj obj) {
        // Reference for stream of bytes
        byte[] stream = null;
        // ObjectOutputStream is used to convert a Java object into OutputStream
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);) {
            oos.writeObject(obj);
            stream = baos.toByteArray();
        } catch (IOException e) {
            // Error in serialization
            e.printStackTrace();
        }
        return stream;
    }

	public static MessageObj toMsgObj(byte[] stream) {
		MessageObj obj = null;

	    try (ByteArrayInputStream bais = new ByteArrayInputStream(stream);
	            ObjectInputStream ois = new ObjectInputStream(bais);) {
	    	obj = (MessageObj) ois.readObject();
	    } catch (IOException e) {
	        // Error in de-serialization
	        e.printStackTrace();
	    } catch (ClassNotFoundException e) {
	        // You are converting an invalid stream to Student
	        e.printStackTrace();
	    }
	    return obj;
	}
}
