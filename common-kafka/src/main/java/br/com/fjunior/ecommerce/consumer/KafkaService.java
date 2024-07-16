package br.com.fjunior.ecommerce.consumer;

import br.com.fjunior.ecommerce.Message;
import br.com.fjunior.ecommerce.dispacher.GsonSerializer;
import br.com.fjunior.ecommerce.dispacher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String,String> properties) {
        this(parse, groupId, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String,String> properties) {
        this(parse, groupId, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction<T> parse, String groupId, Map<String,String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupId, properties));
    }

    public void run() throws ExecutionException, InterruptedException {
        //Pergunta se há mensagem, mas só por um tempo
        try(var deadLetterDispacher = new KafkaDispatcher<>()){
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    System.out.println("\nEncontrei "+records.count()+" registros");
                    for (var record : records) {
                        try {
                            parse.consume(record);
                        } catch (Exception e) {
                            e.printStackTrace();
                            var message = record.value();
                            deadLetterDispacher.send("ECOMMERCE_DEADLETTER", message.getId().toString(),
                                    message.getId().continueWith("DeadLetter"),
                                    new GsonSerializer<>().serialize("",message));
                        }
                    }
                }
            }
        }
    }

    private Properties getProperties(String groupId, Map<String,String> overrideProperties) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        //Precisamos dizer qual é o ID do grupo
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //Adicionando nome ao consumidor
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-" + UUID.randomUUID().toString());
        //Máximo de records a ser consumido.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        //OFFSET "latest" VS "earliest"
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //Override properties
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close(){
        consumer.close();
    }
}
