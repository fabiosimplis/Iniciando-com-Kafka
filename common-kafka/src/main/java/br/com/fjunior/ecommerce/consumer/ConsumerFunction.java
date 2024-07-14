package br.com.fjunior.ecommerce.consumer;

import br.com.fjunior.ecommerce.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction<T> {
    void consume(ConsumerRecord<String, Message<T>> record) throws Exception;
}
