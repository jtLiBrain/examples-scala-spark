package com.jtLiBrain.examples.java.algorithm;

import java.util.Arrays;

/**
 * 1. https://www.jianshu.com/p/68e52bd36024
 * 2. https://blog.csdn.net/elma_tww/article/details/86164674
 */
public class QuickSort {
    public static void sort(int[] data, int fromIndex, int toIndex) {
        if(fromIndex < toIndex) {
            int idx = divideByPivot(data, fromIndex, toIndex);

            sort(data, 0, idx-1);
            sort(data, idx+1, toIndex);
        }
    }

    /**
     * 以数组中指定的开始索引位置的数值为基准值，对 [fromIndex, toIndex] 范围的数组分段进行重新排列，
     * 使得重排后的数据分段满足：基准值左侧数值都小于等于基准值，右侧数值都大于等于基准值
     * @param data
     * @param fromIndex
     * @param toIndex
     * @return 返回基准值所在的索引位置
     */
    public static int divideByPivot(int[] data, int fromIndex, int toIndex) {
        int i = fromIndex;
        int j = toIndex;
        int pivot = data[i]; // 选取第数组中第一个元素为基准值

        while (i < j) {
            while (i < j && data[j] >= pivot) { // 当数组尾部的元素大于等于基准值时，向前挪动 j 指针
                j--;
            }
            data[i] = data[j]; // 这里data[j]是小于基准值的数值，根据快排的思路：基准值左侧的数据小于等于基准值，
                               // 所以，将其赋值给 i 的位置

            while (i < j && data[i] <= pivot) { // 当数组前部的元素小于等于基准值时，向后挪动 i 指针
                i++;
            }
            data[j] = data[i]; // 这里data[i]是大于基准值的数值，根据快排的思路：基准值右侧的数据大于等于基准值，
                               // 所以，将其赋值给 j 的位置
        }

        // 循环结束后，i == j，也就是 i 或 j 为基准值 pivot 的最终的索引位置，所以，最后将基准值赋值到该位置
        data[i] = pivot;

        return i;
    }
}
