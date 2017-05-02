package com.ebsco.kinesis.java.library;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;


/**
 * Created by aganapathy on 5/1/17.
 * This class is responsible to consume blocking queue and sends to AWS kinesis in batches of 3
 */
public class KinesisPublisher implements Runnable{

    private final String STREAM_NAME = "artful_dodgers_demo_stream";

    private final String REGION = "us-east-1";

    protected AmazonKinesis kinesisClient;

    protected List<PutRecordsRequestEntry> entries;

    protected BlockingQueue<TransactionLogging> queue;

    private static final Random RANDOM = new Random();


    /**
     * Constructor to initialize kinesis client
     * @param queue
     */

    public KinesisPublisher(BlockingQueue<TransactionLogging> queue) {
        try {
            this.queue = queue;
            AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
            AWSCredentials awsCredentials = credentialsProvider.getCredentials();
            kinesisClient = AmazonKinesisClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).withRegion(REGION).build();

            entries = new ArrayList<>();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while(queue.size() >= 0){
            try{
                TransactionLogging transactionLogging = queue.take();
                if(isValidated(transactionLogging)) {
                    String explicitHashkey1 = new BigInteger(128, RANDOM).toString(10);
                    String explicitHashkey2= new BigInteger(128, RANDOM).toString(10);
                    String explicitHashkey;
                    ByteBuffer data = ByteBuffer.wrap(transactionLogging.getPayload().getBytes("UTF-8"));
                    if(Integer.parseInt(transactionLogging.getSessionId()) == 9 || Integer.parseInt(transactionLogging.getSessionId()) == 1){
                        explicitHashkey = explicitHashkey1;
                    }else{
                        explicitHashkey = explicitHashkey2;
                    }
                    addEntry(new PutRecordsRequestEntry().withPartitionKey(transactionLogging.getSessionId()).withData(data).withExplicitHashKey(explicitHashkey));
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     * This method validates the incoming transactionLogging object
     * @param transactionLogging
     * @return
     */

    private boolean isValidated(TransactionLogging transactionLogging){
        return true;
    }

    /**
     * This method published to AWS kinesis streams in batch of 3
     */

    protected void flush() {
        PutRecordsResult putRecordsResult = kinesisClient.putRecords(new PutRecordsRequest()
                .withStreamName(STREAM_NAME)
                .withRecords(entries));


        System.out.println(putRecordsResult.getRecords().size()+" records sent to kinesis streams");
        entries.clear();
        putRecordsResult.getRecords().forEach((System.out::println));
        System.out.println("Failed record count "+putRecordsResult.getFailedRecordCount());

        System.out.println("\n************************************************************************\n");


    }

    /**
     * This method watches the count of entries(list) and decides whether to publish or not
     * @param entry
     */
    protected void addEntry(PutRecordsRequestEntry entry) {

        if (entries.size() < 3) {
            entries.add(entry);
        } else {
            flush();
            addEntry(entry);
        }
    }
}
