package com.kafka.avroschemareg.example;

import io.confluent.ksql.avro_schemas.KsqlDataSourceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class TdsConsumer {

//	private SimpMessagingTemplate template;

//	@Autowired
//	public TdsConsumer(SimpMessagingTemplate template) {
//		this.template = template;
//	}


	private final Logger logger = LoggerFactory.getLogger(TdsConsumer.class);

//	@KafkaListener(topics = "TDS_FRS_BB_TRANSACTIONS_ENRICHED")
	public void consume(KsqlDataSourceSchema tds) throws IOException {
		logger.info(String.format("#### -> Consumed message -> %s", tds));
//		template.convertAndSend("/topic/greetings", new Greeting("Hello there: " + order.getFirstName() + " " + order.getLastName()));
	}
}