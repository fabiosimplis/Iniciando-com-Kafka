package br.com.fjunior.ecommerce;

import br.com.fjunior.ecommerce.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;


public class ReadingReportService {

    private final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var reportService = new ReadingReportService();
        try(var service = new KafkaService<>(ReadingReportService.class.getSimpleName(),
                "ECOMMERCE_USER_GENERATE_READING_REPORT",
                reportService::parse,
                new HashMap<>())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<User>> record) throws IOException {

        System.out.println("-----------------------------------------");
        System.out.println("Processing report for " + record.value());

        var message = record.value();
        var user = message.getPayload();
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.getUUID());

        System.out.println("File created " + target.getAbsolutePath());

    }

}
