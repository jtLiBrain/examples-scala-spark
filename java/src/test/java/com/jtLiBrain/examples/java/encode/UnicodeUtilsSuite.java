package com.jtLiBrain.examples.java.encode;

import org.junit.Test;

import java.util.Arrays;

public class UnicodeUtilsSuite {
    @Test
    public void test3() {
        int[] d = UnicodeUtils.toCodePointArray3("中文");
        System.out.println(Arrays.toString(d));
    }
}
