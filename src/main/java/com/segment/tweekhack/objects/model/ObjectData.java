package com.segment.tweekhack.objects.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

@Data
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class ObjectData {
    private String id;
    private Map<String, ?> properties;
}
