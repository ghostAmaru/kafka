package br.com.brmtech.ecommerce;

import ecommerce.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class CreateUserService {

    public static void main(String[] args) {

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
