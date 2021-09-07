package com.qzp.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * HBase操作工具类
 */
public class HBaseUtil {

    private static final ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();

    public static void makeHBaseConnection() throws IOException {

        Connection conn = connHolder.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
            System.out.println("Connection set");
        }

    }


    public static void createTable(String tableName, String[] fields) {
        Connection conn = connHolder.get();

        try {
            assert conn != null;
            HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
            TableName name = TableName.valueOf(tableName);

            if (admin.tableExists(name)) {
                admin.disableTable(name);
                admin.deleteTable(name);
                System.out.println("delete table" + tableName);
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(name);
            for (String column : fields) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(column);
                tableDescriptor.addFamily(columnDescriptor);
            }

            admin.createTable(tableDescriptor);
            System.out.println("create table: " + tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void addRecord(String tableName, String row, String[] fields, String[] values) {
        Connection conn = connHolder.get();

        try {
            assert conn != null;
            Table table = conn.getTable(TableName.valueOf(tableName));

            for (int i = 0; i < fields.length; i++) {
                Put put = new Put(row.getBytes(StandardCharsets.UTF_8));
                String[] cols = fields[i].split(":");
                if (cols.length == 1) {
                    System.err.println("Input Error! Please enter in the format of columnFamily:column");
                } else {
                    put.addColumn(cols[0].getBytes(StandardCharsets.UTF_8), cols[1].getBytes(StandardCharsets.UTF_8), values[i].getBytes(StandardCharsets.UTF_8));
                }

                table.put(put);
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void scanColumn(String tableName, String column) {
        boolean isNull = true;
        Connection conn = connHolder.get();

        try {
            assert conn != null;
            Table table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            String[] cols = column.split(":");
            if (cols.length == 1) {
                scan.addFamily(Bytes.toBytes(column));
            } else {
                scan.addColumn(Bytes.toBytes(cols[0]), Bytes.toBytes(cols[1]));
            }

            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner) {
                isNull = false;
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
                    System.out.println("Timetamp:" + cell.getTimestamp() + " ");
                    System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
                    System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
                    System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
                }
            }

            if (isNull) {
                System.out.println("NULL");
            }

            table.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void modifyData(String tableName, String row, String column, String value) {
        Connection conn = connHolder.get();

        try {
            assert conn != null;
            Table table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(row.getBytes(StandardCharsets.UTF_8));
            String[] cols = column.split(":");
            if (cols.length == 1) {
                System.err.println("Input Error! Please enter in the format of columnFamily:column");
            } else {
                put.addColumn(cols[0].getBytes(StandardCharsets.UTF_8), cols[1].getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
            }
            table.put(put);
            System.out.println("modify success");
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteRow(String tableName, String row) {
        Connection conn = connHolder.get();

        try {
            assert conn != null;
            Table table = conn.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(row.getBytes(StandardCharsets.UTF_8));
            table.delete(delete);
            System.out.println("delete success");
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() throws IOException {
        Connection conn = connHolder.get();
        if (conn != null) {
            conn.close();
            connHolder.remove();
        } else
            System.out.println("Connection does not exist!");
    }
}