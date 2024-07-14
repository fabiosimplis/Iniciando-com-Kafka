package br.com.fjunior.ecommerce;

import br.com.fjunior.ecommerce.dispacher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrder {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try(var orderDispacher = new KafkaDispatcher<Order>()) {
            try (var emailDispacher = new KafkaDispatcher<Email>()) {

                var email = Math.random() + "@email.com";
                for (var i = 0; i < 10; i++) {

                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);
                    var order = new Order(orderId, amount, email);
                    var id = new CorrelationId(NewOrder.class.getSimpleName());
                    orderDispacher.send("ECOMMERCE_NEW_ORDER", email, id, order);

                    Email emailMessage = new Email("Ecommerce","Thank you for your order " + orderId + "!\nWe are processing your order!");
                    emailDispacher.send("ECOMMERCE_SEND_EMAIL", email, id, emailMessage);
                }
            }
        }
    }


}
