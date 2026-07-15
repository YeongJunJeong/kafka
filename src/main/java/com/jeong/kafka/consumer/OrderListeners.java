package com.jeong.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderListeners {

    @KafkaListener (topics = "order-created", groupId = "inventory")
    public void inventory(String msg) {
        System.out.println("[재고] " + msg);
    }

    @KafkaListener(topics = "order-created", groupId = "payment")
    public void payment(String msg) {
        System.out.println("[결제] " + msg);
    }

    @KafkaListener(topics = "order-created", groupId = "shipping")
    public void shipping(String msg) {
        System.out.println("[배송] " + msg);
    }

    @KafkaListener(topics = "order-created", groupId = "points")
    public void points(String msg) {
        System.out.println("[포인트] " + msg);
    }

    @KafkaListener(topics = "order-created", groupId = "buyer-noti")
    public void buyerNoti(String msg) {
        System.out.println("[구매자알림] " + msg);
    }

    @KafkaListener(topics = "order-created", groupId = "seller-noti")
    public void sellerNoti(String msg) {
        System.out.println("[판매자알림] " + msg);
    }

}
