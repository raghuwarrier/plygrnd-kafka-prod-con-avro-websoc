package com.kafka.avroschemareg.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@SpringBootApplication
@RestController
public class SpringKafkaRegistryApplication {
	@Autowired
	private SimpMessagingTemplate template;


	final static Logger logger = Logger.getLogger(SpringKafkaRegistryApplication.class);
//	@Value("${bootstrap.url}")
	String bootstrap;
//	@Value("${registry.url}")
	String registry;

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaRegistryApplication.class, args);
			
	}
	
	@RequestMapping("/greetings")
	public String doIt(@RequestParam("firstName") String firstName, @RequestParam("lastName") String lastName)
	{
		
		String ret=firstName;
		try
		{
			ret += "<br>Using Bootstrap : " + bootstrap;
			ret += "<br>Using Bootstrap : " + registry;
			
			Properties properties = new Properties();
			// Kafka Properties
			properties.setProperty("bootstrap.servers", bootstrap);
			properties.setProperty("acks", "all");
			properties.setProperty("retries", "10");
			// Avro properties
			properties.setProperty("key.serializer", StringSerializer.class.getName());
			properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
			properties.setProperty("schema.registry.url", registry);
			
			ret += sendMsg(properties, "Order-avro", firstName, lastName);
		}
		catch(Exception ex){ ret+="<br>"+ex.getMessage();}
		
		return ret;
	}
	
	private Order sendMsg(Properties properties, String topic, String firstName, String lastName){
		Producer<String, Order> producer = new KafkaProducer<String, Order>(properties);

        Order order = Order.newBuilder()
        		.setOrderId("OId234")
        		.setCustomerId("CId432")
        		.setSupplierId("SId543")
                .setItems(4)
                .setFirstName(firstName)
                .setLastName(lastName)
                .setPrice(178f)
                .setWeight(75f)
                .build();

        ProducerRecord<String, Order> producerRecord = new ProducerRecord<String, Order>(topic, order);

        
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    logger.info(metadata); 
                } else {
                	logger.error(exception.getMessage());
                }
            }
        });

        producer.flush();
        producer.close();
        
        return order;
	}

	@MessageMapping("/hello")
	public void sendGreetings(Greeting greeting){
		template.convertAndSend("/topic/greetings", new Greeting(greeting.getName()));
	}
	
	
}
