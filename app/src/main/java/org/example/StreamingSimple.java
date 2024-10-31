/*
This file is taken from https://github.com/swayvil/simple-beam-jmsio/tree/master
then modified to fit my needs.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.joda.time.Duration;
import javax.jms.ConnectionFactory;
import javax.jms.TextMessage;
import java.io.IOException;
import org.example.Credentials;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.transforms.MapElements;
import javax.jms.Message;
import com.solacesystems.jcsmp.BytesXMLMessage;

public class StreamingSimple {
  public interface Options extends PipelineOptions {
    @Description("Solace-User")
    @Default.String("")
    String getSolaceUser();

    void setSolaceUser(String solaceUser);

    @Description("Solace-Password")
    @Default.String("")
    String getSolacePassword();

    void setSolacePassword(String solacePassword);

    @Description("Solace-URL")
    @Default.String("")
    String getSolaceURL();

    void setSolaceURL(String solaceUrl);

    @Description("Solace-Word-Count-Read-Queue")
    @Default.String("")
    String getSolaceReadQueue();

    void setSolaceReadQueue(String solaceReadQueue);
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    ConnectionFactory solaceConnectionFactory = new JmsConnectionFactory(Credentials.getUsername(), Credentials.getPassword(), Credentials.getHostname());

    Pipeline pipeline = Pipeline.create(options);

   // PCollection<JmsBytesMessage> byteMessages = pipeline.apply("ReadFromJms", JmsIO.read().withConnectionFactory(solaceConnectionFactory).withQueue(Credentials.getQueueName()));
        /*.apply("TransformJmsRecordAsPojo", ParDo.of(new DoFn<JmsRecord, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            System.out.println("Message received:");
            System.out.println(c.element().getPayload());
            System.out.println("\n");
            c.output(c.element().getPayload());
          }
        }));*/

    /*PCollection<TextMessage> textMessages = byteMessages.apply(MapElements.into(TypeDescriptors.of(TextMessage.class))
        .via(message -> {
          try {
            byte[] payload = new byte[(int) message.getBodyLength()];
            message.readBytes(payload);
            //TextMessage textMessage = solaceConnectionFactory.createTextMessage(); // Assuming solaceConnectionFactory can create TextMessages
            //textMessage.setText(new String(payload, StandardCharsets.UTF_8));
            return message;
          } catch (Exception e) {
            throw new RuntimeException("Error converting JMS message", e);
          }
        }));*/
    pipeline.run().waitUntilFinish();
    /*
    PipelineResult result = pipeline.run();
    try {
      result.waitUntilFinish();
    } catch (Exception exc) {
      result.cancel();
    }

     */
  }
}