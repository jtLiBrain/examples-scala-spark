package com.jtLiBrain.examples.java.encode;

/**
 * https://www.ibm.com/developerworks/cn/java/unicode-programming-language/
 * https://www.ibm.com/developerworks/cn/java/j-unicode/
 */
public class UnicodeUtils {
    public static int[] toCodePointArray3(String str) {
        int len = str.length(); // 字符串长度
        int[] acp = new int[str.codePointCount(0, len)]; // 指定文本范围中 Unicode 代码点的数量

        for (int i = 0, j = 0; i < len; i = str.offsetByCodePoints(i, 1)) {
            acp[j++] = str.codePointAt(i);
        }
        return acp;
    }
}
