package br.com.fjunior.ecommerce;

public interface ServiceFactory<T> {
    ConsumerService<T> create();
}
