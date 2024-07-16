package br.com.fjunior.ecommerce.consumer;

public interface ServiceFactory<T> {
    ConsumerService<T> create();
}
