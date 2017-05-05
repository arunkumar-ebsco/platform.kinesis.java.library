import com.ebsco.kinesis.java.library.KPLKinesisPublisher;
import com.ebsco.kinesis.java.library.TransactionLogging;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by aganapathy on 5/1/17.
 * This is client stub code that produces to blocking queue and starts the consumer thread
 */
public class Client {

    public static void main(String[] args) {

        BlockingQueue<TransactionLogging> queue = new ArrayBlockingQueue<>(10);

        final ExecutorService exec = Executors.newCachedThreadPool();
        final KPLKinesisPublisher consumer = new KPLKinesisPublisher(queue);
        exec.execute(consumer);


        try {
            for (int i = 1; i < 3; i++) {
                queue.offer(new TransactionLogging(String.valueOf(i), new String("Sample Payload ")));
                Thread.sleep(2000); // Sleep for 2 seconds and then enqueue
            }
        }catch(InterruptedException e){
            e.printStackTrace();
        }




    }
}
