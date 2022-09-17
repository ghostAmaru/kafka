package br.com.brmtech.ecommerce;

import ecommerce.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class CreateUserService {

    CreateUserService() throws SQLException {
        String url = "jdbc.sqlite:users_database.db";
        Connection connection = DriverManager.getConnection(url);
        connection.createStatement().execute("create table Users (" + "uuid varchar(200) primary key," + "email varchar (200))");
    }

    public static void main(String[] args) throws SQLException {

        var fraudService = new CreateUserService();
        try (var service = new KafkaService<>(CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Order.class,
                Map.of())) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("--------------------------------------");
        System.out.println("Thank you for your order! cheking for new user!");
        System.out.println(record.value());
        var order = record.value();
    }

}
