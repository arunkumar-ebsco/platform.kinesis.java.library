package com.ebsco.kinesis.java.library;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

/**
 * Created by aganapathy on 5/1/17.
 * This class is responsible to consume blocking queue and sends to AWS kinesis in batches of 3
 */
public class KinesisPublisher implements Runnable{

    private final static Logger LOGGER = Logger.getLogger(KinesisPublisher.class.getName());

    private final String STREAM_NAME = "artful_dodgers_demo_stream";

    private final String REGION = "us-east-1";

    protected AmazonKinesis kinesisClient;

    protected List<PutRecordsRequestEntry> entries;

    protected BlockingQueue<TransactionLogging> queue;



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
                    ByteBuffer data = ByteBuffer.wrap(transactionLogging.getPayload().getBytes("UTF-8"));
                    addEntry(new PutRecordsRequestEntry().withPartitionKey(transactionLogging.getSessionId()).withData(data));
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
        if(transactionLogging != null) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    /**
     * This method published to AWS kinesis streams in batch of 3
     */

    protected void flush(){
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(STREAM_NAME);
        StreamDescription streamDescription = kinesisClient.describeStream(describeStreamRequest).getStreamDescription();
        if(null != streamDescription){
            if ("ACTIVE".equalsIgnoreCase(streamDescription.getStreamStatus())) {
                PutRecordsResult putRecordsResult = kinesisClient
                        .putRecords(new PutRecordsRequest().withStreamName(STREAM_NAME).withRecords(entries));

                System.out.println(putRecordsResult.getRecords().size() + " records sent to kinesis streams");
                entries.clear();
                putRecordsResult.getRecords().forEach(System.out::println);
                System.out.println("Failed record count " + putRecordsResult.getFailedRecordCount());

                System.out.println("\n************************************************************************\n");
            } else {
                System.out.println("Stream is not active");
            }
        }




    }

    /**
     * This method watches the count of entries(list) and decides whether to publish or to add to entries list
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
