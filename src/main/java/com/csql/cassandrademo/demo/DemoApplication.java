package com.csql.cassandrademo.demo;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.io.FileReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;


@SpringBootApplication
public class DemoApplication {
    static Cluster cluster;

    protected static Session connect(String hostname, String keyspace) {
        return connect(hostname, 9042, keyspace);
    }

    protected static synchronized Session connect(String hostname, int port, String keyspace) {
        if (cluster == null) {
            cluster = Cluster.builder().addContactPointsWithPorts(Collections.singleton(
                    new InetSocketAddress(hostname, port))).withoutMetrics()
                    .build();
        }

        return cluster.connect(keyspace);
    }

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
        CqlSessionBuilder builder = CqlSession.builder();
        builder.addContactPoint(new InetSocketAddress("localhost", 9042));
        builder.withLocalDatacenter("datacenter1");
        builder.withKeyspace("myKeyspace");
        CqlSession session = builder.build();

        System.out.println(new Date());
        int i = 0;
        try {

            FileReader filereader = new FileReader("/Users/ravi/Downloads/data/dummydata.csv");
            CSVReader csvReader = new CSVReaderBuilder(filereader)
                    .withSkipLines(1)
                    .build();
            String[] nextRecord;
            int j = 0;

            int permits = 1000;
            Semaphore l = new Semaphore(permits);

            List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>(1000);
            while ((nextRecord = csvReader.readNext()) != null) {
                i++;
                l.acquire();
                PreparedStatement prepared = session.prepare("insert into emp(emp_id, emp_name) values (?,?)");
                BoundStatement bound = prepared.bind(Integer.parseInt(nextRecord[0]), nextRecord[1]);
                session.executeAsync(bound)
                        .thenAccept(asyncResultSet -> l.release());
            }
            l.acquire(permits);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Exception");
        }
        System.out.println("records=" + i);
        System.out.println(new Date());

    }
}
