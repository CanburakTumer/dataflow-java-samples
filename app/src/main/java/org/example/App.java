// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package org.example;

import org.example.Credentials;

import java.io.ByteArrayInputStream;
import java.lang.invoke.TypeDescriptor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.io.solace.SolaceIO;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.io.solace.data.Solace.Queue;
import org.apache.beam.sdk.io.solace.broker.BasicAuthJcsmpSessionServiceFactory;
import org.apache.beam.sdk.io.solace.broker.BasicAuthSempClientFactory;

public class App {
    public interface Options extends StreamingOptions {

        // TODO change to getoptions for Solace Connection
        @Description("Input text to print.")
        @Default.String("My input text")
        String getInputText();

        void setInputText(String value);
    }

    public static void main(String[] args) {
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        var pipeline = Pipeline.create(options);

        // TODO change to get these from PipelineOptions
        String hostname = Credentials.getHostname(true);
        String hostname_with_protocol = Credentials.getHostname(true);
        String semp_hostname = Credentials.getSempHost();
        String username = Credentials.getUsername();
        String password = Credentials.getPassword();
        String vpnname = Credentials.getVpnName();

        System.out.println(hostname);
        // TODO resolve connection error
        try {
            PCollection<Record> events = pipeline.apply("Read from Solace",
                SolaceIO.read().from(Queue.fromName(Credentials.getQueueName())).
                    withSempClientFactory(
                        BasicAuthSempClientFactory.builder()
                            .host(semp_hostname)
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
            PCollection<String> messages = events.apply(MapElements.into(TypeDescriptors.strings()).via(x -> {
                    ByteBuffer singleMessage = x.getPayload();
                    return StandardCharsets.UTF_8.decode(singleMessage).toString();
                }))
                .apply("Print elements",
                    MapElements.into(TypeDescriptors.strings()).via(x -> {
                        System.out.println(x);
                        return x;
                    }));
        } catch (Exception e){
            e.printStackTrace();
        }

        pipeline.run().waitUntilFinish();
    }
}
