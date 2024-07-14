package br.com.fjunior.ecommerce;

import br.com.fjunior.ecommerce.consumer.KafkaService;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ServiceProvider {

    public <T> void run(ServiceFactory<T> factory) throws ExecutionException, InterruptedException {
        var myService = factory.create();

        try(var service = new KafkaService(myService.getConsumerGroup(),
                myService.getTopic(),
                myService::parse,
                Map.of())) {
            service.run();
        }
    }
}
