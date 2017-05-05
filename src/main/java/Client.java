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




        try {
            for (int i = 1; i < 5; i++) {
                queue.add(new TransactionLogging(String.valueOf(i), String.valueOf(i).concat(" payload")));
                //Thread.sleep(2000); // Sleep for 2 seconds and then enqueue
            }
            final ExecutorService exec = Executors.newCachedThreadPool();
            final KPLKinesisPublisher consumer = new KPLKinesisPublisher(queue);
            exec.execute(consumer);
        }catch(Exception e){
            e.printStackTrace();
        }




    }
}
