package br.com.fjunior.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

class KafkaService {

    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;
    private final String simpleName;

    KafkaService(String simpleName, String topic, ConsumerFunction parse) {
        this.simpleName = simpleName;
        this.consumer = new KafkaConsumer(properties(simpleName));
        this.parse = parse;
        consumer.subscribe(Collections.singletonList(topic));
    }


    void run(){
        //Pergunta se há mensagem, mas só por um tempo
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("Encontrei "+records.count()+" registros");
                for (var record : records) {
                    parse.consume(record);
                }
            }
        }
    }

    private static Properties properties(String groupId) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //Precisamos dizer qual é o ID do grupo
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //Adicionando nome ao consumidor
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-" + UUID.randomUUID().toString());
        //Máximo de records a ser consumido.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }
}
