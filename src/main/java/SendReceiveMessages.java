//snippet-sourcedescription:[<<FILENAME>> demonstrates how to send, receive and delete messages from a queue.]
//snippet-keyword:[Java]
//snippet-sourcesyntax:[java]
//snippet-keyword:[Code Sample]
//snippet-keyword:[Amazon Simple Queue Service]
//snippet-service:[sqs]
//snippet-sourcetype:[full-example]
//snippet-sourcedate:[]
//snippet-sourceauthor:[soo-aws]
/*
 * Copyright 2011-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://aws.amazon.com/apache2.0
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and
 * limitations under the License.
 */

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.util.Date;
import java.util.List;

public class SendReceiveMessages {
    private static final String QUEUE_NAME = "Tenant_status.fifo";

    public static void main(String[] args) {
        final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

//        try {
//            CreateQueueResult create_result = sqs.createQueue(QUEUE_NAME);
//        } catch (AmazonSQSException e) {
//            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
//                throw e;
//            }
//        }
        String queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();

        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody("shradha:started")
                .withMessageDeduplicationId("6")
                .withMessageGroupId("Tenant_status");
        sqs.sendMessage(send_msg_request);


//        sendBulkMessages(sqs, queueUrl);

        getMessages2(sqs, queueUrl);
    }

    private static void getMessages(AmazonSQS sqs, String queueUrl) {
        // receive messages from the queue
        ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(queueUrl);

        List<Message> messages = sqs.receiveMessage(queueUrl)
                .getMessages();
        System.out.println("message count: " + messages.size());
        // delete messages from the queue
        for (Message m : messages) {
            System.out.println(m.getBody());
//            sqs.deleteMessage(queueUrl, m.getReceiptHandle());
        }
    }


    private static void getMessages2(AmazonSQS sqs, String queueUrl) {
        // receive messages from the queue
//        ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(queueUrl);
        ReceiveMessageRequest receive_request = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                ;
        receive_request.setMaxNumberOfMessages(10);
        List<Message> messages = sqs.receiveMessage(receive_request)
                .getMessages();
        System.out.println("message count: " + messages.size());
        // delete messages from the queue
        for (Message m : messages) {
            System.out.println(m.getBody());
//            sqs.deleteMessage(queueUrl, m.getReceiptHandle());
        }
    }

    private static void sendBulkMessages(AmazonSQS sqs, String queueUrl) {
        // Send multiple messages to the queue
        SendMessageBatchRequest send_batch_request = new SendMessageBatchRequest()
                .withQueueUrl(queueUrl)
                .withEntries(
                        new SendMessageBatchRequestEntry(
                                "msg_1", "gaurav:stopped")
                                .withMessageGroupId("Tenant_status"),
                        new SendMessageBatchRequestEntry(
                                "msg_2", "gaurav:crashed")
                                .withMessageGroupId("Tenant_status"));
        sqs.sendMessageBatch(send_batch_request);
    }
}