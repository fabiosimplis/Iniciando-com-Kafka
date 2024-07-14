package br.com.fjunior.ecommerce;

import br.com.fjunior.ecommerce.consumer.KafkaService;
import br.com.fjunior.ecommerce.dispacher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;


public class EmailNewOrderService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var fraudService = new EmailNewOrderService();
        try(var service = new KafkaService<>(EmailNewOrderService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                new HashMap<>())) {
            service.run();
        }
    }

    private final KafkaDispatcher<String> emailDispacher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {

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

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
