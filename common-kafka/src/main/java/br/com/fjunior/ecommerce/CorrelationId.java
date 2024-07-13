package br.com.fjunior.ecommerce;

import java.util.UUID;

public class CorrelationId {

    private final String id;

    public CorrelationId(String tittle) {
        this.id = tittle + "("+UUID.randomUUID().toString()+")";
    }

    @Override
    public String toString() {
        return "CorrelationId{" +
                "id='" + id + '\'' +
                '}';
    }

    public CorrelationId continueWith(String tittle) {
        return new CorrelationId(id + "-" + tittle);
    }
}
