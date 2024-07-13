package br.com.fjunior.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;


public class FraudDetectorService {

    public static void main(String[] args) {

        var fraudService = new FraudDetectorService();
        try(var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                new HashMap<>())) {
            service.run();
        }
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {

        System.out.println("-----------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("Key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        var message = record.value();
        var order = message.getPayload();
        if (isFraud(order)){
            // pretending that the fraud happens when the amount is >= 4500
            System.out.println("Order is a fraud!!!");
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()),order);
        } else {
            System.out.println("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        }


    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
