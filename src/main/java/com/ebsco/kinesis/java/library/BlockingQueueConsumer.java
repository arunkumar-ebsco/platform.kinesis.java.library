package com.ebsco.kinesis.java.library;

import java.util.concurrent.BlockingQueue;

/**
 * Created by aganapathy on 5/1/17.
 */
public class BlockingQueueConsumer implements Runnable{

    protected BlockingQueue<TransactionLogging> queue;

    public BlockingQueueConsumer(BlockingQueue<TransactionLogging> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while(queue.size() >= 0){
            try{
                System.out.println(queue.take());
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }
        System.out.println("End");
    }
}
