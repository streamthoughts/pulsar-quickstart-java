/*
 * Copyright 2019 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.pulsar;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A simple Apache Pulsar client for producing messages using batches.
 */
public class BatchingProducer {

    public static void main(final String[] args) throws PulsarClientException {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("my-first-topic")
                .compressionType(CompressionType.SNAPPY)
                .enableBatching(true)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(1000)
                .create();

        for (int i = 0; i < 10 * 1000; i++) {
            TypedMessageBuilder<String> message = producer.newMessage()
                .value("Hello Streams Word")
                .property("application", "pulsar-java-quickstart")
                .property("pulsar.client.version", "2.4.1")
                .property("message.num", String.valueOf(i));
            CompletableFuture<MessageId> future = message.sendAsync();
            future.thenAccept(msgId -> {
                System.out.printf("Message with ID %s successfully sent asynchronously\n", msgId);
            });
        }
        producer.close();
        client.close();
    }
}
