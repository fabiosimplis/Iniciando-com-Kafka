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
        try(var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                new HashMap<>())) {
            service.run();
        }
    }

    private final KafkaDispatcher<User> userDispacher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws ExecutionException, InterruptedException, IOException, SQLException {

        System.out.println("-----------------------------------------");
        System.out.println("Processing new batch");
        var message = record.value();
        System.out.println("Topic: " + message.getPayload());

        for(User user : getAllUsers()){
            userDispacher.sendAsync(message.getPayload(), user.getUUID(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),user);
            System.out.println("Enviado para " + user);
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
