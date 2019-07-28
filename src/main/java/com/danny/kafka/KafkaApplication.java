package com.danny.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.danny.kafka.model.Customer;

@SpringBootApplication
@EnableKafka
public class KafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}
	
	@Bean
    public ProducerFactory<String, Customer> producerFactory() {
		String bootstrapAddress = "127.0.0.1:9092";
		Map<String, Object> configProps = new HashMap<>();
        configProps.put(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
          bootstrapAddress);
        configProps.put(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
          StringSerializer.class);
//        configProps.put(
//          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
//          StringSerializer.class);
        configProps.put(
              ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
              JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }
 
    @Bean
    public KafkaTemplate<String, Customer> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        String bootstrapAddress = "127.0.0.1:9092";
        String groupId = "java-group";
    	Map<String, Object> props = new HashMap<>();
        props.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
          bootstrapAddress);
        props.put(
          ConsumerConfig.GROUP_ID_CONFIG, 
          groupId);
        props.put(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
          StringDeserializer.class);
        props.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
          StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }
 
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> 
      kafkaListenerContainerFactory() {
    
        ConcurrentKafkaListenerContainerFactory<String, String> factory
          = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
      filterKafkaListenerContainerFactory() {
     
        ConcurrentKafkaListenerContainerFactory<String, String> factory
          = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setRecordFilterStrategy(
          record -> record.value().contains("World"));
        return factory;
    }

    public ConsumerFactory<String, Customer> greetingConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "greeting");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(Customer.class));
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Customer> greetingKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Customer> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(greetingConsumerFactory());
        return factory;
    }
    
}
