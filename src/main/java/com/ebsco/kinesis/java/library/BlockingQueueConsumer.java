package com.ebsco.kinesis.java.library;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by aganapathy on 5/1/17.
 * This class is responsible to consume blocking queue and sends to AWS kinesis in batches of 3
 */
public class BlockingQueueConsumer implements Runnable{

    private String STREAM_NAME = "arun_test_stream";

    protected AmazonKinesis kinesis;

    protected List<PutRecordsRequestEntry> entries;

    protected BlockingQueue<TransactionLogging> queue;

    public BlockingQueueConsumer(BlockingQueue<TransactionLogging> queue) {
        this.queue = queue;
        AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception ex) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct ", ex);
        }
        kinesis = new AmazonKinesisClient(credentialsProvider);
        entries = new ArrayList<>();
    }

    @Override
    public void run() {
        while(queue.size() >= 0){
            try{
                TransactionLogging transactionLogging = queue.take();
                if(isValidated(transactionLogging)) {
                    String partitionKey = transactionLogging.getSessionId();
                    ByteBuffer data = ByteBuffer.wrap(transactionLogging.getPayload().getBytes("UTF-8"));

                    addEntry(new PutRecordsRequestEntry().withPartitionKey(partitionKey).withData(data));
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
        PutRecordsResult putRecordsResult = kinesis.putRecords(new PutRecordsRequest()
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
