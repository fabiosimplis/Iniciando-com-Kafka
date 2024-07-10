package br.com.fjunior.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrder {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try(var orderDispacher = new KafkaDispatcher<Order>()) {
            try (var emailDispacher = new KafkaDispatcher<Email>()) {

                var email = Math.random() + "@email.com";
                for (var i = 0; i < 10; i++) {
                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);
                    var order = new Order(userId, orderId, amount, email);
                    orderDispacher.send("ECOMMERCE_NEW_ORDER", userId, order);

                    Email emailMessage = new Email("Ecommerce","Thank you " + userId + " for your order " + orderId + "!\nWe are processing your order!");
                    emailDispacher.send("ECOMMERCE_SEND_EMAIL", userId, emailMessage);
                }
            }
        }
    }


}
