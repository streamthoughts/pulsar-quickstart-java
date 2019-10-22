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

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.concurrent.CompletableFuture;

/**
 * A simple Apache Pulsar client for producing messages.
 */
public class SimpleProducer {

    public static void main(final String[] args) throws PulsarClientException {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("my-first-topic")
                .create();

        // Synchronously a byte[] message
        MessageId id = producer.send("Hello Streams Word!");
        System.out.printf("Message with ID %s successfully sent synchronously\n", id);

        CompletableFuture<MessageId> future = producer.sendAsync("Make sense of streams processing");
        future.thenAccept(msgId -> {
            System.out.printf("Message with ID %s successfully sent asynchronously\n", msgId);
        });

        TypedMessageBuilder<String> message = producer.newMessage()
                .key("my-key")
                .value("value-message")
                .property("application", "pulsar-java-quickstart")
                .property("pulsar.client.version", "2.4.1");
        message.send();

        future.join();

        producer.close();
        client.close();
    }
}
