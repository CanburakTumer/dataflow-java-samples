// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
package org.example;

import org.example.Credentials;

import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.apache.beam.sdk.io.solace.broker.BasicAuthJcsmpSessionServiceFactory;
import org.apache.beam.sdk.io.solace.broker.BasicAuthSempClientFactory;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.io.solace.data.Solace.Queue;
import org.apache.beam.sdk.io.solace.data.Solace;

public class App {
    public interface Options extends StreamingOptions {
        @Description("SEMP Hostname")
        @Default.String("http://localhost")
        String getSempHostname();
        void setSempHostname(String value);

        @Description("JCSMP Hostname")
        @Default.String("http://localhost")
        String getJcsmpHostname();
        void setJcsmpHostname(String value);

        @Description("Username")
        @Default.String("username")
        String getUsername();
        void setUsername(String value);

        @Description("Password")
        @Default.String("*****")
        String getPassword();
        void setPassword(String value);

        @Description("VPN Name")
        @Default.String("default")
        String getVpnName();
        void setVpnName(String value);

        @Description("Queue name")
        @Default.String("my-queue")
        String getQueueName();
        void setQueueName(String value);

    }

    public static void main(String[] args) {
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        var pipeline = Pipeline.create(options);

        // TODO change to get these from PipelineOptions
        String hostname = options.getJcsmpHostname();
        String sempHostname = options.getSempHostname();
        String username = options.getUsername();
        String password = options.getPassword();
        String vpnname = options.getVpnName();
        String queueName = options.getQueueName();

        try {
            PCollection<Record> events = pipeline.apply("Read from Solace",
                SolaceIO.read().from(Queue.fromName(queueName)).
                    withSempClientFactory(
                        BasicAuthSempClientFactory.builder()
                            .host(sempHostname)
                            .username(username)
                            .password(password)
                            .vpnName(vpnname)
                            .build())
                    .withSessionServiceFactory(
                    BasicAuthJcsmpSessionServiceFactory.builder()
                        .host(hostname)
                        .username(username)
                        .password(password)
                        .vpnName(vpnname)
                        .build()));
            PCollection<Solace.Record> messages = events.apply(
                "PassThrough",
                MapElements.via(
                    new SimpleFunction<Solace.Record, Solace.Record>() {
                        @Override
                        public Solace.Record apply(Solace.Record s) {
                            String str = StandardCharsets.UTF_8.decode(s.getPayload()).toString();
                            System.out.println(str);
                            return s;
                        }
                    }));
        } catch (Exception e){
            e.printStackTrace();
        }

        pipeline.run().waitUntilFinish();
    }
}
