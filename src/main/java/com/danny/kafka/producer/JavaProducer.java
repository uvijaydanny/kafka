package com.danny.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.danny.kafka.model.Customer;

@RestController
public class JavaProducer {

	private String topicName = "first-topic";
	
	@Autowired
	private KafkaTemplate<String, Customer> kafkaTemplate;
	 
	@GetMapping("/send")
	public void sendMessage(@RequestParam String msg) {
		System.out.println("Message received = " + msg);
		Customer cust = new Customer();
		cust.setId("1");
		cust.setName(msg);
		
	    ListenableFuture<SendResult<String, Customer>> listenableFuture = kafkaTemplate.send(topicName, cust);
	    listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, Customer>>() {
	    	 
	        @Override
	        public void onSuccess(SendResult<String, Customer> result) {
	        	System.out.println("Partition " + result.getRecordMetadata().partition());
	            System.out.println("Sent message=[" + msg + 
	              "] with offset=[" + result.getRecordMetadata().offset() + "]");
	        }
	        @Override
	        public void onFailure(Throwable ex) {
	            System.out.println("Unable to send message=["
	              + msg + "] due to : " + ex.getMessage());
	        }
	    });
	}
}
