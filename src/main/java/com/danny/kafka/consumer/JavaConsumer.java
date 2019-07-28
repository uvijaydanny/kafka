package com.danny.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.danny.kafka.model.Customer;

@Component
public class JavaConsumer {

	@KafkaListener(topics = "first-topic", groupId = "java-group",containerFactory="greetingKafkaListenerContainerFactory")
	public void listen(@Payload Customer cust,
		@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
	    System.out.println("Received Messasge from partition: " + partition);
	    System.out.println("Received Messasge in group foo: " + cust.getId() + " " + cust.getName());
	}
	
}
