package br.com.fjunior.ecommerce;

public class User {

    private final String uuid;

    public User(String uuid) {
        this.uuid = uuid;
    }

    public String getUUID() {
        return uuid;
    }
}
