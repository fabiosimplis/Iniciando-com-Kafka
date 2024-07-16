package br.com.fjunior.ecommerce;

import br.com.fjunior.LocalDatabase;
import br.com.fjunior.ecommerce.consumer.ConsumerService;
import br.com.fjunior.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserService implements ConsumerService<Order> {

    private final LocalDatabase database;

    CreateUserService() throws SQLException {
        this.database = new LocalDatabase("users_database");
        this.database.createIfNotExists("CREATE TABLE Users (" +
                "uuid varchar(200) primary key," +
                "email varchar(200))");
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

        var uuid = UUID.randomUUID().toString();

        database.update("INSERT INTO Users (uuid, email)" +
                "values (?,?)", uuid, email);

        System.out.println("User " + uuid +  " e "+ email +" adicionado");
    }

    private boolean isNewUser(String email) throws SQLException {

        var results = database.query("SELECT uuid FROM Users " +
                "WHERE email = ? LIMIT 1", email);

        return !results.next();
    }

}
