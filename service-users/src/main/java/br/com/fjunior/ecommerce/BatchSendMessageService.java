package br.com.fjunior.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;

    BatchSendMessageService() throws SQLException {
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

    public static void main(String[] args) throws SQLException {

        var batchService = new BatchSendMessageService();
        try(var service = new KafkaService<>(CreateUserService.class.getSimpleName(),
                "SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                String.class,
                new HashMap<>())) {
            service.run();
        }
    }

    private final KafkaDispatcher<User> userDispacher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, String> record) throws ExecutionException, InterruptedException, IOException, SQLException {

        System.out.println("-----------------------------------------");
        System.out.println("Processing new batch");
        System.out.println("Topic: " + record.value());

        for(User user : getAllUsers()){
            userDispacher.send(record.value(), user.getUUID(), user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var result = connection.prepareStatement("SELECT uuid FROM Users").executeQuery();
        List<User> users = new ArrayList<>();
        while (result.next()){
            users.add(new User(result.getString(1)));
        }
        return users;
    }
}
