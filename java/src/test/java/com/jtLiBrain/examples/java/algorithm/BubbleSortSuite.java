package com.jtLiBrain.examples.java.algorithm;

import org.junit.Test;

import java.util.Arrays;

public class BubbleSortSuite {
    @Test
    public void testBubbleSort() {
        int[] data = { 5, 4, 3, 2, 1 };
        BubbleSort.sort2(data);
        System.out.println(Arrays.toString(data));
    }
}
