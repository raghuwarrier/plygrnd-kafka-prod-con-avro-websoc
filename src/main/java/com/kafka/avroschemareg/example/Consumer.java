package com.kafka.avroschemareg.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class Consumer {

//	private SimpMessagingTemplate template;

//	@Autowired
//	public Consumer(SimpMessagingTemplate template) {
//		this.template = template;
//	}


	private final Logger logger = LoggerFactory.getLogger(Consumer.class);

//	@KafkaListener(topics = "Order-avro")
	public void consume(Order order) throws IOException {
		logger.info(String.format("#### -> Consumed message -> %s", order));
//		template.convertAndSend("/topic/greetings", new Greeting("Hello there: " + order.getFirstName() + " " + order.getLastName()));
	}
}