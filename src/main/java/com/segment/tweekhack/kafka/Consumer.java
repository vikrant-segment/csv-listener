package com.segment.tweekhack.kafka;

import com.segment.tweekhack.objects.util.RequestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class Consumer {
    private final RequestUtils utils;

    @Autowired
    public Consumer(RequestUtils utils) {
        this.utils = utils;
    }

    @KafkaListener(topics = "test")
    public void consume(String message) throws IOException {
        utils.setObject(message);
    }
}
