package com.segment.tweekhack.objects.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.segment.tweekhack.common.Constants;
import com.segment.tweekhack.objects.ApiClient;
import com.segment.tweekhack.objects.model.ObjectData;
import com.segment.tweekhack.objects.model.Request;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class RequestUtils {

    private final ApiClient client;

    @Autowired
    public RequestUtils(ApiClient client) {
        this.client = client;
    }

    public void setObject(String json) throws JsonProcessingException {
        log.debug("Set object request");
        Map<String, ?> parsed = toMap(json);

        String writeKey = (String) parsed.get(Constants.WRITE_KEY);
        parsed.remove(Constants.WRITE_KEY);
        Request request = toRequest(parsed);
        log.info(request.toString());
        client.setObject(request, writeKey);
    }

    private Request toRequest(Map<String, ?> map) {
        log.trace("Converting map to Request");
        Request request = new Request();
        ObjectData object = new ObjectData();

        object.setId((String) map.get(Constants.ID));
        map.remove(Constants.ID);

        request.setCollection((String) map.get(Constants.COLLECTION));
        map.remove(Constants.COLLECTION);

        object.setProperties(map);
        request.setObjects(List.of(object));

        return request;
    }

    private Map<String, ?> toMap(String json) throws JsonProcessingException {
        log.trace("Converting json to Map");
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, Map.class);
    }
}
