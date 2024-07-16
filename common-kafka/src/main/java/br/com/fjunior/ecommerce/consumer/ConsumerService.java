package br.com.fjunior.ecommerce.consumer;

import br.com.fjunior.ecommerce.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface ConsumerService<T> {

    //You may argue that a ConsumerException would be better
    // and its ok, it can be better
    void parse(ConsumerRecord<String, Message<T>> record) throws Exception;
    String getTopic();
    String getConsumerGroup();
}
