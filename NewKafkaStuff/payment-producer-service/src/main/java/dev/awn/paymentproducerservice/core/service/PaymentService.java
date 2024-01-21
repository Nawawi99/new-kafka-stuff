package dev.awn.paymentproducerservice.core.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class PaymentService {
    private final String PAYMENT_TOPIC = "payment-topic";
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ReplyingKafkaTemplate<String, Object, Object> replyingKafkaTemplate;
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    /** One way Async communication */
//    public void sendMessage() {
//        kafkaTemplate.send(PAYMENT_TOPIC, "This is the first message from the [PAYMENT PRODUCER]");
//    }

    /** Two Way ASYNC Communication */
//    public void sendMessage() {
//        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(PAYMENT_TOPIC, "[P] Hello");
//
//        CompletableFuture.runAsync(() -> {
//            try {
//                RequestReplyFuture<String, Object, Object> replyFuture = replyingKafkaTemplate.sendAndReceive(producerRecord);
//                SendResult<String, Object> sendResult = replyFuture.getSendFuture().get(100, TimeUnit.SECONDS);
//                ConsumerRecord<String, Object> consumerRecord = replyFuture.get(100, TimeUnit.SECONDS);
//
//                System.out.println(consumerRecord.value());
//
//            } catch (Exception ignored) {
//            }
//        }).thenRun(() -> System.out.println("This will be executed after response is received"));
//
//        System.out.println("This will not get blocked");
//    }

    /**
     * Two-way sync communication req/res
     */
    public void sendMessage() {
        // Preparing the message
        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(PAYMENT_TOPIC, "[P] Hello");

        try {
            RequestReplyFuture<String, Object, Object> replyFuture = replyingKafkaTemplate.sendAndReceive(producerRecord);
            SendResult<String, Object> sendResult = replyFuture.getSendFuture().get(100, TimeUnit.SECONDS);
            ConsumerRecord<String, Object> consumerRecord = replyFuture.get(100, TimeUnit.SECONDS);

            // This value() is the response received
            System.out.println(consumerRecord.value());
        } catch (Exception ignored) {
        }


        System.out.println("This will get blocked");
    }
}
