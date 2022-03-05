//snippet-sourcedescription:[<<FILENAME>> demonstrates how to enable long polling when creating a queue.]
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
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

public class LongPolling
{
    private static final String QUEUE_NAME = "Tenant_status.fifo";

    public static void main(String[] args)
    {
        final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        String queue_url = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();

        // Enable long polling on an existing queue
        SetQueueAttributesRequest set_attrs_request = new SetQueueAttributesRequest()
                .withQueueUrl(queue_url)
                .addAttributesEntry("ReceiveMessageWaitTimeSeconds", "20");
        sqs.setQueueAttributes(set_attrs_request);

        // Enable long polling on a message receipt
        ReceiveMessageRequest receive_request = new ReceiveMessageRequest()
                .withQueueUrl(queue_url)
                .withWaitTimeSeconds(20);

        sqs.receiveMessage(receive_request);
    }
}