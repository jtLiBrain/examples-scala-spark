package com.jtLiBrain.examples.java.algorithm;

import org.junit.Test;

import java.util.Arrays;

public class QuickSortSuite {
    @Test
    public void testQuickSort() {
        int[] data = { 23, 45, 17, 11, 13, 89, 72, 26, 3, 17, 11, 13 };
        QuickSort.sort(data, 0, data.length-1);
        System.out.println(Arrays.toString(data));
    }
}
