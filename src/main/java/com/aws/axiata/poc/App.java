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
        /*
        if( args.length < 1 ) {
            System.err.println( "java -jar ra-1.0-SNAPSHOT.jar <path to property file" );
            return;
        }
        */
        args = new String[ 1 ];
        args[0] = "/Users/rspamzn/Documents/DAML/Customers/Axiata/poc/app/resources/app.properties";
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
            record.addProperty( "msisdn", msisdn );
            record.addProperty( "session_start_timestamp", new java.util.Date().getTime() );
            record.addProperty( "session_end_timestamp", new java.util.Date().getTime() + 10000 );
            record.addProperty( "cell_id", "23AB" );
            record.addProperty( "lac_id", "12AB" );
            record.addProperty( "volume_uplink", 234 );
            record.addProperty( "volume_downlink", 3244 );
            record.addProperty( "option_id", "package1233" );
            record.addProperty( "data_quota_balance", 3244 );
            record.addProperty( "data_quota_total", 56000000 );
            record.addProperty( "charge", 30 );

            System.out.println( "Streaming Record : " + record.toString() );
            producer.send( new ProducerRecord<String, String>( topic, msisdn, record.toString() ) );
        }
        System.out.println( "Finished Streaming " + numRecords + " Records" );
        producer.close();
    }
}
