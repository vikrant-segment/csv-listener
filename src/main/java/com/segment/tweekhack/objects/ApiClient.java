package com.segment.tweekhack.objects;

import com.segment.tweekhack.objects.model.Request;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
@Slf4j
public class ApiClient {
    @Value(value = "${objects.api.url}")
    private String url;

    @Value(value = "${objects.api.context.path}")
    private String uri;

    public void setObject(Request request, String writeKey) {
        log.info("Sending set object request to Segment");
        WebClient webClient = WebClient.create(url);
        webClient.post()
                .uri(uri)
                .header(HttpHeaders.AUTHORIZATION, "Basic " + writeKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .bodyToMono(Void.class)
                .block();
    }
}
