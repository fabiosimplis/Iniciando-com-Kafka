package br.com.fjunior.ecommerce;

import br.com.fjunior.ecommerce.dispacher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrder {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try(var orderDispacher = new KafkaDispatcher<Order>()) {
                var email = Math.random() + "@email.com";
                for (var i = 0; i < 10; i++) {
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);
                    var order = new Order(orderId, amount, email);
                    var id = new CorrelationId(NewOrder.class.getSimpleName());
                    orderDispacher.send("ECOMMERCE_NEW_ORDER", email, id, order);
                }
        }
    }


}
