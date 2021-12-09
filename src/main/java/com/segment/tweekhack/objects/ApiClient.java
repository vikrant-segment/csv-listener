package com.segment.tweekhack.objects;

import com.segment.tweekhack.objects.model.Request;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class ApiClient {
    private ExchangeFilterFunction requestLoggingFilter = ExchangeFilterFunction.ofRequestProcessor(clientRequest -> {
        log.info("Sending request to Segment - Method: {}, Headers: {}, URL: {} ",
                clientRequest.method().name(),
                clientRequest.headers(),
                clientRequest.url());
        return Mono.just(clientRequest);
    });

    private ExchangeFilterFunction responseLoggingFilter = ExchangeFilterFunction.ofResponseProcessor(clientResponse -> {
        log.info("Received response from Segment- Headers: {}, Status: {}", clientResponse.headers().asHttpHeaders().toString(), clientResponse.statusCode());
        return Mono.just(clientResponse);
    });

    @Value(value = "${objects.api.url}")
    private String url;

    @Value(value = "${objects.api.context.path}")
    private String uri;

    public Void setObject(Request request, String writeKey) {
        log.info("Sending set object request to Segment");
        WebClient webClient = WebClient.
                builder()
                .baseUrl(url)
                .filter(requestLoggingFilter)
                .filter(responseLoggingFilter)
                .build();
        return webClient.post()
                .uri(uri)
                .header(HttpHeaders.AUTHORIZATION, "Basic " + writeKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .bodyToMono(Void.class)
                .block();
    }
}
