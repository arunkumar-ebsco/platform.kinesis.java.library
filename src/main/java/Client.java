import com.ebsco.kinesis.java.library.KinesisPublisher;
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

        BlockingQueue<TransactionLogging> queue = new ArrayBlockingQueue<TransactionLogging>(10);

        final ExecutorService exec = Executors.newCachedThreadPool();
        final KinesisPublisher consumer = new KinesisPublisher(queue);
        exec.execute(consumer);


        try {
            for (int i = 1; i < 11; i++) {
                queue.put(new TransactionLogging(String.valueOf(i), new String("Sample Payload ")));
            }
        }catch(InterruptedException e){
            e.printStackTrace();
        }




    }
}
