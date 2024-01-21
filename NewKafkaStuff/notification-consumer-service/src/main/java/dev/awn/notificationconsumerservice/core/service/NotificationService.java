package dev.awn.notificationconsumerservice.core.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {
    private final String PAYMENT_TOPIC = "payment-topic";
    private final String NOTIFICATION_RESPONSE_TOPIC = "notification-response-topic";

    /** One way Async Communication */
//    @KafkaListener(topics = PAYMENT_TOPIC)
//    public void sendNotification(String message) {
//        System.out.println("Notification Consumer have received the message from [PAYMENT PRODUCER]");
//        System.out.println("The message " +
//                "*************************");
//        System.out.println(message);
//        System.out.println("*************************");
//    }

    /** Two way ASYNC Commucation */
    @KafkaListener(topics = PAYMENT_TOPIC)
    @SendTo(NOTIFICATION_RESPONSE_TOPIC)
    public String sendNotification(String message) {
        System.out.println(message);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return "[C] Hi";
    }


}
