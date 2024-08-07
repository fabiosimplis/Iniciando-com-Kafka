package br.com.fjunior.ecommerce;

public class Message<T> {

    private final CorrelationId id;
    private final T payload;

    public Message(CorrelationId id, T payload) {
        this.id = id;
        this.payload = payload;
    }

    public CorrelationId getId() {
        return id;
    }

    public T getPayload() {
        return payload;
    }
}
