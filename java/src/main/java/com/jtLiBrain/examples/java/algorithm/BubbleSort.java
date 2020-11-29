package com.jtLiBrain.examples.java.algorithm;

/**
 * https://www.jianshu.com/p/648d87dc4cfc
 * https://www.cnblogs.com/bigdata-stone/p/10464243.html
 * https://www.toutiao.com/a6593273307280179715/?iid=6593273307280179715
 * https://www.jianshu.com/p/1458abf81adf
 */
public class BubbleSort {
    private static void swap(int[] array, int i, int j) {
        int temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }

    public static int[] sort1(int[] array) {
        if (array == null || array.length < 2) {
            return array;
        }

        int len = array.length;

        for (int i = 0; i < len - 1; i++) { // N 个数字要排序完成，总共进行 N-1 趟排序，每趟找出最大数，并将其往后移动
            // 第 i 趟排序的比较次数为 (N-1)-i 次，"减i"是因为每进行一趟排序都会找出一个较大值，这样上一趟找到最大值肯定不比本次的最大值小，所以无需与上一次做比较
            for (int j = 0; j < len - 1 - i; j++) {
                if (array[j] > array[j + 1]) {
                    swap(array, j, j+1);
                }
            }
        }

        return array;
    }

    public static int[] sort2(int[] array) {
        if (array == null || array.length < 2) {
            return array;
        }

        int len = array.length;

        for (int i = len - 1; i > 0; i--) {
            for (int j = 0; j < i; j++) {
                if (array[j] > array[j + 1]) {
                    swap(array, j, j+1);
                }
            }
        }

        return array;
    }
}
