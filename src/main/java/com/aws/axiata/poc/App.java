package com.aws.axiata.poc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.gson.JsonObject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class App {
    public static void main( String[] args ) throws IOException {
        if( args.length < 1 ) {
            System.err.println( "java -jar ra-1.0-SNAPSHOT.jar <path to property file" );
            return;
        }
        
        InputStream input = new FileInputStream( args[ 0 ] );
        Properties prop = new Properties();
        prop.load( input );

        Properties kafkaProps = new Properties();
        kafkaProps.put( "bootstrap.servers", prop.getProperty( "bootstrap_servers" ) );
        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", 0);
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("linger.ms", 1);
        kafkaProps.put("buffer.memory", 33554432);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>( kafkaProps );
        int numRecords = Integer.parseInt( prop.getProperty( "num_records" ) );
        String startIndex = prop.getProperty( "starting_index" );
        String strFormat = "%0" + Integer.toString( numRecords ).length() + "d";
        String topic = prop.getProperty( "topic_name" );

        for( int i=1; i<=numRecords; i++ ) {
            String msisdn = startIndex + String.format( strFormat, i );
            JsonObject record = new JsonObject();
            //{ "msisdn": "2088aed6dcab216cdcc71", "session_start_timestamp": 1614477032000, "session_end_timestamp": 1614480632000, 
            //"cell_id": "cell1", "lac_id": "lac1", "volume_uplink": 1234, 
            //"volume_downlink": 234, "option_id": "pack1", "data_quota_balance": 8, 
            //"data_quota_total": 100, "charge": 10, "event" : "cdr" }
            int max = 9;
            int min = 0; 
            if( i>5 ) {
                max = 10;
                min = 100;
            }
            int range = max - min + 1;
            double quotaBalance = (int)(Math.random() * range) + min;
            record.addProperty( "msisdn", msisdn );
            record.addProperty( "session_start_timestamp", new java.util.Date().getTime() );
            record.addProperty( "session_end_timestamp", new java.util.Date().getTime() + 10000 );
            record.addProperty( "cell_id", "23AB" );
            record.addProperty( "lac_id", "12AB" );
            record.addProperty( "volume_uplink", 234 );
            record.addProperty( "volume_downlink", 3244 );
            record.addProperty( "option_id", "just4ME" );
            record.addProperty( "data_quota_balance", quotaBalance );
            record.addProperty( "data_quota_total", 100 );
            record.addProperty( "charge", 30 );
            record.addProperty( "event", "cdr" );

            System.out.println( "Streaming Record : " + record.toString() );
            producer.send( new ProducerRecord<String, String>( topic, msisdn, record.toString() ) );
        }
        System.out.println( "Finished Streaming " + numRecords + " Records" );
        producer.close();
    }
}
