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

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.bean.ProducerBean;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import java.util.Properties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DataStreamJob {


  public static void main(String[] args) throws Exception {

    DebeziumSourceFunction<String> source = MySqlSource.<String>builder()
        .hostname("localhost")
        .port(3306)
        .databaseList("auth_center,data_center,hhnail") //数据库用逗号分隔
        .tableList("auth_center.department,auth_center.role,hhnail.staff") //库.表 逗号分隔
        .username("root")
        .password("root")
        .deserializer(
            new StringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
        .startupOptions(StartupOptions.initial()) // 全量bin log
//        .startupOptions(StartupOptions.latest()) // 增量bin log
        .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<String> dataStreamSource = (DataStreamSource<String>) env.addSource(source);
    dataStreamSource.print();

//    dataStreamSource.addSink(new MyMqSink());
    env.execute("localCDC");
  }

  private static Properties getMqProperties() {
    Properties properties = new Properties();
    properties.setProperty(PropertyKeyConst.AccessKey, "xxx");
    properties.setProperty(PropertyKeyConst.SecretKey, "xxx");
    properties.setProperty(PropertyKeyConst.ONSAddr,
        "xxxxx");
    return properties;
  }

  public static class MyMqSink extends RichSinkFunction<String> {

    private ProducerBean producerBean;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      producerBean = new ProducerBean();
      producerBean.setProperties(getMqProperties());
      producerBean.start();
      System.out.println("成功");
    }

    @Override
    public void close() throws Exception {
      boolean closed = producerBean.isClosed();
      if (!closed) {
        producerBean.shutdown();
      }
      super.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
      Message message = new Message();
      message.setTopic("flink");
      message.setBody(JSON.toJSONBytes("GG"));
      message.setTag("tag");
      producerBean.send(message);
      System.out.println("发送mq成功,内容:"+ value);
      super.invoke(value, context);
    }


  }

}
