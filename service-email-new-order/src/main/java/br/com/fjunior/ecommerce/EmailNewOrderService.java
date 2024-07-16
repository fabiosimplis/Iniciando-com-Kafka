package br.com.fjunior.ecommerce;

import br.com.fjunior.ecommerce.consumer.ConsumerService;
import br.com.fjunior.ecommerce.consumer.ServiceRunner;
import br.com.fjunior.ecommerce.dispacher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;


public class EmailNewOrderService implements ConsumerService<Order> {

    public static void main(String[] args)  {
        new ServiceRunner(EmailNewOrderService::new).start(1);

    }

    private final KafkaDispatcher<String> emailDispacher = new KafkaDispatcher<>();

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {

        System.out.println("-----------------------------------------");
        System.out.println("Processing new order, preparing email");
        System.out.println("value: " + record.value());

        var emailCode = "Thank you for your order! We are processing your order!";
        var order = record.value().getPayload();
        CorrelationId id = record.value().getId().continueWith(EmailNewOrderService.class.getSimpleName());
        emailDispacher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(),
                id,
                emailCode);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
