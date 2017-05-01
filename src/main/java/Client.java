import com.ebsco.kinesis.java.library.BlockingQueueConsumer;
import com.ebsco.kinesis.java.library.TransactionLogging;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by aganapathy on 5/1/17.
 */
public class Client {

    public static void main(String[] args) {

        BlockingQueue<TransactionLogging> queue = new ArrayBlockingQueue<TransactionLogging>(10);



        try {
            for (int i = 1; i < 11; i++) {
                queue.put(new TransactionLogging(String.valueOf(i), new String("Sample Payload ")));
            }
        }catch(InterruptedException e){
            e.printStackTrace();
        }

        final ExecutorService exec = Executors.newCachedThreadPool();
        final BlockingQueueConsumer consumer = new BlockingQueueConsumer(queue);
        exec.execute(consumer);
        System.out.println(consumer);

    }
}
