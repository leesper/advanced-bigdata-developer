package com.kaikeba.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.runner.notification.RunListener;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseOperate {
    private Connection connection;
    private final String TABLE_NAME = "myuser";
    private Table table;

    @Test
    public void createTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        TableName myuser = TableName.valueOf("myuser");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(myuser);

        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        HColumnDescriptor f2 = new HColumnDescriptor("f2");

        hTableDescriptor.addFamily(f1);
        hTableDescriptor.addFamily(f2);

        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();
    }

    @BeforeTest
    public void initTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf(TABLE_NAME));
    }

    @AfterTest
    public void close() throws IOException {
        table.close();
        connection.close();
    }

    @Test
    public void addData() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Put put = new Put("0001".getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), "zhangsan".getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(18));
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(25));
        put.addColumn("f1".getBytes(), "address".getBytes(), Bytes.toBytes("EarthMan"));
        table.put(put);
        table.close();
    }

    @Test
    public void batchInsert() throws IOException {
        Put put = new Put("0002".getBytes());
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("caocao"));
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(30));
        put.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("peiguoqiaoxian"));
        put.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("helloworld"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("liubei"));
        put2.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put2.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("youzhouzhuojunzhuoxian"));
        put2.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("talk is cheap , show me the code"));

        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("sunquan"));
        put3.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(35));
        put3.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("xiapei"));
        put3.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("12888888888"));
        put3.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("what are you doing"));

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("zhugeliang"));
        put4.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put4.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("sichuan longzhong"));
        put4.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("14888888888"));
        put4.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("can you remember me"));

        Put put5 = new Put("0006".getBytes());
        put5.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("simayi"));
        put5.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(27));
        put5.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("哪里人有待考究"));
        put5.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15888888888"));
        put5.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("跟诸葛亮死掐"));

        Put put6 = new Put("0007".getBytes());
        put6.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put6.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("xiaobubu—吕布"));
        put6.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put6.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put6.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("内蒙人"));
        put6.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15788888888"));
        put6.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("貂蝉去哪了"));

        List<Put> putList = new ArrayList<>();
        putList.add(put);
        putList.add(put2);
        putList.add(put3);
        putList.add(put4);
        putList.add(put5);
        putList.add(put6);

        table.put(putList);
    }

    @Test
    public void getData() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Get get = new Get(Bytes.toBytes("0003"));
        get.addFamily("f1".getBytes());
        get.addColumn("f2".getBytes(), "phone".getBytes());
        Result result = table.get(get);
        List<Cell> cells = result.listCells();

        for (Cell cell : cells) {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] column = CellUtil.cloneQualifier(cell);
            byte[] rowkey = CellUtil.cloneRow(cell);
            byte[] value = CellUtil.cloneValue(cell);
            System.out.println(Bytes.toString(family));
            System.out.println(Bytes.toString(column));
            System.out.println(Bytes.toString(rowkey));
            if ("age".equals(Bytes.toString(column)) || "id".equals(Bytes.toString(column))) {
                System.out.println(Bytes.toInt(value));
            } else {
                System.out.println(Bytes.toString(value));
            }
        }
        table.close();
    }

    @Test
    public void scanData() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        scan.addFamily("f1".getBytes());
        scan.addColumn("f2".getBytes(), "phone".getBytes());
        scan.setStartRow("0003".getBytes());
        scan.setStopRow("0007".getBytes());
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for(Cell cell : cells) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                if("age".equals(Bytes.toString(qualifier))  || "id".equals(Bytes.toString(qualifier))){
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) +
                            "======数据的列族为" + Bytes.toString(family) +
                            "======数据的列名为" + Bytes.toString(qualifier) +
                            "==========数据的值为" + Bytes.toInt(value));
                }else{
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) +
                            "======数据的列族为" + Bytes.toString(family) +
                            "======数据的列名为" + Bytes.toString(qualifier) +
                            "==========数据的值为" + Bytes.toString(value));
                }
            }
        }
        table.close();
    }

    @Test
    public void rowFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        BinaryComparator binaryComparator = new BinaryComparator("0003".getBytes());
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, binaryComparator);

        scan.setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                //判断id和age字段，这两个字段是整形值
                if ("age".equals(Bytes.toString(qualifier_name))  || "id".equals(Bytes.toString(qualifier_name))) {
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) +
                            "======数据的列族为" + Bytes.toString(family_name) +
                            "======数据的列名为" +  Bytes.toString(qualifier_name) +
                            "==========数据的值为" +Bytes.toInt(value));
                } else {
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) +
                            "======数据的列族为" + Bytes.toString(family_name) +
                            "======数据的列名为" + Bytes.toString(qualifier_name) +
                            "==========数据的值为" + Bytes.toString(value));
                }
            }
        }
    }

    @Test
    public void familyFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("f2");
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(familyFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                if ("age".equals(Bytes.toString(qualifier_name)) || "id".equals(Bytes.toString(qualifier_name))) {
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) +
                            "======数据的列族为" + Bytes.toString(family_name) +
                            "======数据的列名为" + Bytes.toString(qualifier_name) +
                            "==========数据的值为" + Bytes.toInt(value));
                }else{
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) +
                            "======数据的列族为" + Bytes.toString(family_name) +
                            "======数据的列名为" + Bytes.toString(qualifier_name) +
                            "==========数据的值为" + Bytes.toString(value));
                }
            }
        }
    }

    @Test
    public void qualifierFilter() throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("name");
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(qualifierFilter);
        ResultScanner scanner = table.getScanner(scan);
        printResult(scanner);
    }

    private void printResult(ResultScanner scanner) {
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                if ("age".equals(Bytes.toString(qualifier_name)) || "id".equals(Bytes.toString(qualifier_name))) {
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) +
                            "======数据的列族为" + Bytes.toString(family_name) +
                            "======数据的列名为" + Bytes.toString(qualifier_name) +
                            "==========数据的值为" + Bytes.toInt(value));
                }else{
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) +
                            "======数据的列族为" + Bytes.toString(family_name) +
                            "======数据的列名为" + Bytes.toString(qualifier_name) +
                            "==========数据的值为" + Bytes.toString(value));
                }
            }
        }
    }

    @Test
    public void contains8() throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("8");
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(valueFilter);
        ResultScanner scanner = table.getScanner(scan);
        printResult(scanner);
    }

    @Test
    public void singleColumnValueFilter() throws IOException {
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(),
                "name".getBytes(), CompareFilter.CompareOp.EQUAL, "liubei".getBytes());
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        printResult(scanner);
    }

    @Test
    public void prefixFilter() throws IOException {
        Scan scan = new Scan();
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        scan.setFilter(prefixFilter);
        ResultScanner scanner = table.getScanner(scan);
        printResult(scanner);
    }

    @Test
    public void hbasePageFilter() throws IOException {
        int pageNum= 3;
        int pageSize = 2;
        Scan scan = new Scan();
        if(pageNum == 1 ){
            //获取第一页的数据
            scan.setMaxResultSize(pageSize);
            scan.setStartRow("".getBytes());
            //使用分页过滤器来实现数据的分页
            PageFilter filter = new PageFilter(pageSize);
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            printResult(scanner);
        }else{
            String  startRow = "";
            //扫描数据的调试 扫描五条数据
            int scanDatas = (pageNum - 1) * pageSize + 1;
            scan.setMaxResultSize(scanDatas);//设置一步往前扫描多少条数据
            PageFilter filter = new PageFilter(scanDatas);
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                byte[] row = result.getRow();//获取rowkey
                //最后一次startRow的值就是0005
                startRow= Bytes.toString(row);//循环遍历我们多有获取到的数据的rowkey
                //最后一条数据的rowkey就是我们需要的起始的rowkey
            }
            //获取第三页的数据
            scan.setStartRow(startRow.getBytes());
            scan.setMaxResultSize(pageSize);//设置我们扫描多少条数据
            PageFilter filter1 = new PageFilter(pageSize);
            scan.setFilter(filter1);
            ResultScanner scanner1 = table.getScanner(scan);
            printResult(scanner1);
        }
    }

    @Test
    public void filterList() throws IOException {
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(),
                "name".getBytes(), CompareFilter.CompareOp.EQUAL, "liubei".getBytes());
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(prefixFilter);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        printResult(scanner);
    }

    @Test
    public  void  deleteData() throws IOException {
        Delete delete = new Delete("0003".getBytes());
        table.delete(delete);
    }

    @Test
    public void deleteTable() throws IOException {
        //获取管理员对象，用于表的删除
        Admin admin = connection.getAdmin();
        //删除一张表之前，需要先禁用表
        admin.disableTable(TableName.valueOf(TABLE_NAME));
        admin.deleteTable(TableName.valueOf(TABLE_NAME));
    }
}
