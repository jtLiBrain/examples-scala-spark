package com.jtLiBrain.examples.java.collection;

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MapExample {
    @Test
    public void testStream() {
        Map<Integer, String> datum = new LinkedHashMap<Integer, String>();
        datum.put(1, "v1");
        datum.put(2, "v2");
        datum.put(3, "v3");

        List result = datum.entrySet().stream().map((e) ->{
            return e.getValue();
        }).collect(Collectors.toList());
    }
}
