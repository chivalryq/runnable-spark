package com.qzp.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.spark.HBaseContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class InputData {
    public static void main(String[] args) throws IOException {
        {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            Connection connection= ConnectionFactory.createConnection(conf);
            System.out.println("connection set");
            Admin admin = connection.getAdmin();

            TableName tName = TableName.valueOf("test");
            if (admin.tableExists(tName)) {
                System.out.println("hello test");
            } else {
                System.out.println("no test");
            }

            String hbaseSource = "org.apache.hadoop.hbase.spark";
            String  hbaseTable = "Person";

            SparkSession spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("SparkByExample")
                    .getOrCreate();
            Dataset<Row> hbaseDF = spark.read().format(hbaseSource)
                    .option("hbase.columns.mapping",
                            "rowKey STRING :key," +
                            "firstName STRING Name:First, lastName STRING Name:Last," +
                            "country STRING Address:Country, state STRING Address:State"
                    )
                    .option("hbase.table",hbaseTable).load();

            hbaseDF.schema();
            System.out.println("printing schema");
            System.out.println(hbaseDF.schema());
//  import org.apache.spark.SparkConf
//  import org.apache.spark.api.java.JavaSparkContext
//  import org.apache.spark.sql.SQLContext
//
//  var conf = new SparkConf().setAppName("SparkHbaseConnectorPOC").setMaster(sparkMaster)
//
//  var javaSparkContext = new JavaSparkContext(conf)
//  javaSparkContext.hadoopConfiguration.set("spark.hbase.host", sparkHbaseHost)
//  javaSparkContext.hadoopConfiguration.set("spark.hbase.port", sparkHbasePort)
//
//  var sqlContext = new SQLContext(javaSparkContext)
        }
    }
}
