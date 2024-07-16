package br.com.fjunior.ecommerce;

import br.com.fjunior.ecommerce.consumer.ConsumerService;
import br.com.fjunior.ecommerce.consumer.KafkaService;
import br.com.fjunior.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserService implements ConsumerService<Order> {

    private final Connection connection;

    private CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        connection = DriverManager.getConnection(url);
        try {
               connection.createStatement().execute("CREATE TABLE Users (" +
                    "uuid varchar(200) primary key," +
                    "email varchar(200))");
        } catch (SQLException ex){
            // be careful, the sql could be wrong, be really careful
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {

        new ServiceRunner<>(CreateUserService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException{

        System.out.println("-----------------------------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.println("Key: " + record.key());
        System.out.println("value: " + record.value());
        var message = record.value();
        var order = message.getPayload();
        if (isNewUser(order.getEmail()))
        {
            insertNewUser(order.getEmail());
        }
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    private void insertNewUser(String email) throws SQLException {
        var insert = connection.prepareStatement("INSERT INTO Users (uuid, email)" +
                "values (?,?)");

        var uuid = UUID.randomUUID().toString();
        insert.setString(1, uuid);
        insert.setString(2, email);
        insert.execute();
        System.out.println("User " + uuid +  " e "+ email +" adicionado");
    }

    private boolean isNewUser(String email) throws SQLException {

        var exists = connection.prepareStatement("SELECT uuid FROM Users " +
                "WHERE email = ? LIMIT 1");
        exists.setString(1,email);
        var result = exists.executeQuery();
        return !result.next();
    }

}
