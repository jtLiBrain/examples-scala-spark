package com.jtLiBrain.examples.java.generics;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class GenericsExample {
    /**
     * extends定义的是上界：保读
     *
     * 保读：
     * 因为要保证由此泛型限制的类型读出来的是某预期的类型（此例要读出的类型为Fruit），
     * 所以 (a), (b), (c), (d) 的赋值都是成立的,它们的实际类型ArrayList<Fruit>, ArrayList<Apple>, ArrayList<Jonathan>, ArrayList<Orange>
     * 都能保证实际读出来的类型一定是Fruit（Fruit的子类当然是Fruit了）；
     * 但(e)如果成立，它实际是可以存放任何Object的，如果其存放了一个String对象，那么将一个String转化为Fruit肯定是会出现运行时错误的，所以(e)赋值不成立
     *
     * 限写：
     * 因为 (a), (b), (c), (d)的成立，所以ArrayList<Fruit>, ArrayList<Apple>, ArrayList<Jonathan>, ArrayList<Orange>都是可以赋值给List<? extends Fruit>的，
     * 这时如果一个实例类型ArrayList<Apple>的List，被添加入了一个Orange，这显然是不合理的，
     * 所以编译器是不允许(f), (g), (h)的
     */
    @Test
    public void testExtends() {
        List<? extends Fruit> flist1 = new ArrayList<Fruit>();    //(a)
        List<? extends Fruit> flist2 = new ArrayList<Apple>();    //(b)
        List<? extends Fruit> flist3 = new ArrayList<Jonathan>(); //(c)
        List<? extends Fruit> flist4 = new ArrayList<Orange>();   //(d)
        //List<? extends Fruit> flist5 = new ArrayList<Object>(); //(e)

        //flist1.add(new Fruit());                                //(f)
        //flist1.add(new Apple());                                //(g)
        //flist1.add(new Object());                               //(h)

        List<Orange> flist6 = new ArrayList<Orange>();
        flist6.add(new Orange());
        List<? extends Fruit> flist7 = flist6;

        Fruit fruit = flist7.get(0);
    }

    /**
     * super定义下界：保写
     *
     * 保写：
     * 因为要保证由此泛型约束的类型的数据被顺利写入（此例要写入的类型为Fruit），
     * 所以(b), (c)的赋值都是成立的，它们的实际类型ArrayList<Fruit>, ArrayList<Object>，
     * 都能保证实际能写入Fruit（Fruit的子类当然是Fruit了）；
     * 但(a)如果成立，它实际只能存放Apple对象，如果同为Fruit的Orange对象被存入，那么肯定是会出现运行时错误的，所以(a)赋值不成立
     *
     * 宽松读：
     * 因为 (b), (c) 成立，所以ArrayList<Fruit>, ArrayList<Object>都是可以赋值给List<? super Fruit>的，
     * 这时如果一个实例类型为ArrayList<Fruit>的List，被添加入了一个Object，这显然是不合理的，
     * 所以编译器是不允许(f)的
     */
    @Test
    public void testSuper() {
        //List<? super Fruit> flist1 = new ArrayList<Apple>(); //(a)
        List<? super Fruit> flist2 = new ArrayList<Fruit>();   //(b)
        List<? super Fruit> flist3 = new ArrayList<Object>();  //(c)

        flist2.add(new Fruit());                               //(d)
        flist2.add(new Apple());                               //(e)
        //flist2.add(new Object());                            //(f)

        Object obj = flist2.get(0);                            //(g)
    }
}

class Fruit {}
class Apple extends Fruit{}
class Jonathan extends Apple{}
class Orange extends Fruit{}