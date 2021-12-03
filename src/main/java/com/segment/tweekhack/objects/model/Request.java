package com.segment.tweekhack.objects.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@Data
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class Request {
    private String collection;
    private List<ObjectData> objects;
}
