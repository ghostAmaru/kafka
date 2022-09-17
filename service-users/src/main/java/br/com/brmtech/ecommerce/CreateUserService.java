package br.com.brmtech.ecommerce;

import ecommerce.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class CreateUserService {

    public static void main(String[] args) {

        var fraudService = new CreateUserService();
        try (var service = new KafkaService<>(CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER", fraudService::parse, Order.class, Map.of())) {
            service.run();
        }
    }

    //    Para enviar mensagens basta criar um DISPATCHER
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();


    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("--------------------------------------");
        System.out.println("Thank you for your order! We are processing your order!");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        var order = record.value();
        if (isFraud(order)) {
            //             pretending that the fraud happens when the amount is >= 4500 - fingindo que a fraude acontece com 4500
            System.out.println("Order is a FRAUD!!!" + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getUserId(), order);

        } else {
            System.out.println("Order processed-Aprovada  : " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APROVED", order.getUserId(), order);
        }
    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
