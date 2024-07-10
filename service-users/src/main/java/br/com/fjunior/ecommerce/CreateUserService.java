package br.com.fjunior.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        connection = DriverManager.getConnection(url);
        connection.createStatement().execute("CREATE TABLE Users ("+
                "uuid varchar(200) primary key," +
                "email varchar(200))");
    }

    public static void main(String[] args) throws SQLException {

        var createUserService = new CreateUserService();
        try(var service = new KafkaService<Order>(CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                Order.class,
                new HashMap<String, String>())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) throws SQLException {

        System.out.println("-----------------------------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.println("Key: " + record.key());
        System.out.println("value: " + record.value());
        var order = record.value();
        if (isNewUser(order.getEmail()))
        {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var insert = connection.prepareStatement("INSERT INTO Users (uuid, email)" +
                "values (?,?)");

        insert.setString(1, "uuid");
        insert.setString(2, email);
        insert.execute();
        System.out.println("User uuid e" + email + " adicionado");
    }

    private boolean isNewUser(String email) throws SQLException {

        var exists = connection.prepareStatement("SELECT uuid FROM Users" +
                "WHERE email = ? LIMIT 1");
        exists.setString(1,email);
        var result = exists.executeQuery();
        return !result.next();
    }

}
