package com.haizhi.test;

import org.junit.Test;

public class StringBuilderTest {

    public void testString() {
        long start = System.currentTimeMillis();
        String str = null;
        for (int i = 0; i < 20000; i++) {
            str = str + i + ",";
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    public void testStringNew() {
        long start = System.currentTimeMillis();

        for (int i = 0; i < 20000; i++) {
            String str = null;
            str = str + i + ",";
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    public void testStringBuffer() {
        long start = System.currentTimeMillis();

        StringBuffer sbuf = new StringBuffer();
        for (int i = 0; i < 20000; i++) {
            sbuf.append(i + ",");
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    public void testStringBufferNew() {
        long start = System.currentTimeMillis();

        for (int i = 0; i < 20000; i++) {
            StringBuffer sbuf = new StringBuffer();
            sbuf.append(i + ",");
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    public void testStringBulider() {
        long start = System.currentTimeMillis();

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 20000; i++) {
            builder.append(i + ",");
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    public void testStringBuliderNew() {
        long start = System.currentTimeMillis();

        for (int i = 0; i < 20000; i++) {
            StringBuilder builder = new StringBuilder();
            builder.append(i + ",");
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void test() {
        testString();
        testStringNew();
        testStringBuffer();
        testStringBufferNew();
        testStringBulider();
        testStringBuliderNew();
    }

}
