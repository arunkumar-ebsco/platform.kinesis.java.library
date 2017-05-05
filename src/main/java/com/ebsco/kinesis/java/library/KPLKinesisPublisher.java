package com.ebsco.kinesis.java.library;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

/**
 * Created by aganapathy on 5/4/17. This class uses amazon kinesis producer
 * library to publish to kinesis
 */

public class KPLKinesisPublisher implements Runnable {

    private final String STREAM_NAME = "artful_dodgers_demo_stream";

    protected final static String REGION = "us-east-1";

    protected final BlockingQueue<TransactionLogging> inputQueue;

    private final KinesisProducer kinesis;

    public KPLKinesisPublisher(BlockingQueue<TransactionLogging> inputQueue) {
        this.inputQueue = inputQueue;
        AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        AWSCredentials awsCredentials = credentialsProvider.getCredentials();
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        config.setRegion(REGION);
        config.setMaxConnections(1);
        config.setRecordMaxBufferedTime(15000);
        config.setCredentialsProvider(credentialsProvider);

        kinesis = new KinesisProducer(config);

    }



    @Override
    public void run() {
        try {
            TransactionLogging transactionLogging = inputQueue.take();
            String partitionKey = transactionLogging.getSessionId();
            String payload = transactionLogging.getPayload();
            ByteBuffer data = ByteBuffer.wrap(payload.getBytes("UTF-8"));
            ListenableFuture<UserRecordResult> f =
                    kinesis.addUserRecord(STREAM_NAME, partitionKey, data);

            Futures.addCallback(f, new FutureCallback<UserRecordResult>() {
                @Override
                public void onSuccess(UserRecordResult result) {

                    System.out.println((String.format(
                                "Succesfully put record, partitionKey=%s, "
                                        + "payload=%s, sequenceNumber=%s, "
                                        + "shardId=%s",
                                partitionKey, payload, result.getSequenceNumber(),
                                result.getShardId()
                                )));

                }

                @Override
                public void onFailure(Throwable t) {
                    if (t instanceof UserRecordFailedException) {
                        UserRecordFailedException e =
                                (UserRecordFailedException) t;
                       
                        e.printStackTrace();
                        System.out.println(String.format(
                                "Record failed to put, partitionKey=%s, "
                                        + "payload=%s",
                                partitionKey, payload));
                    }
                };
            });
        }
            
        catch (Exception e){
            e.printStackTrace();
        }


    }
}
