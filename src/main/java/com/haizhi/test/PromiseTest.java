package com.haizhi.test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PromiseTest {
    public static void main(String[] args) throws Throwable {
        // 两个线程的线程池
        ExecutorService executor = Executors.newFixedThreadPool(2);
        //jdk1.8之前的实现方式
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            System.out.println("task started!");
            try {
                //模拟耗时操作
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return "task finished!";
        }, executor);

        //采用lambada的实现方式
        future.thenAccept(e -> System.out.println(e + " ok"));

        System.out.println("main thread is running");
    }
}
