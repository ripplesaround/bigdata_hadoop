import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class lab3 {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    public static void main(String[] args)throws IOException{
        init();

//        createTable("test_student",new String[]{"score"});
//        insertData("test_student","zhangsan","score","English","69");
//        insertData("test_student","zhangsan","score","Math","86");
//        insertData("test_student","zhangsan","score","Computer","77");
//        getData("test_student", "zhangsan", "score","English");
//        delData("test_student", "score","English", "zhangsan");
        deltable("test_student");
        Table_scan("test_student");
        close();
    }

    public static void init(){
        configuration  = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void createTable(String myTableName,String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if(admin.tableExists(tableName)){
            System.out.println("talbe is exists!");
        }else {
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            for(String str:colFamily){
                ColumnFamilyDescriptor family =
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(str)).build();
                tableDescriptor.setColumnFamily(family);
            }
            admin.createTable(tableDescriptor.build());
        }
    }

    public static void Table_info(String myTableName) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        HTableDescriptor[] temp = admin.listTables();
        for(HTableDescriptor tem:temp){
            System.out.println(tem.getTableName());
        }
    }

    public static void Table_scan(String myTableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(myTableName));
        System.out.println("Table name: " + myTableName);
        ResultScanner temp = table.getScanner(new Scan());
        int flag=0;
        for( Result tem: temp){
            byte[] rows = tem.getRow();
            System.out.println("row key: "+new String(rows));
            List<Cell> cells = tem.listCells();

            for (Cell temp_cell:cells){
                byte[] fam = temp_cell.getFamilyArray();
                byte[] fam_q = temp_cell.getQualifierArray();
                byte[] value = temp_cell.getValueArray();
                System.out.println("row value: "
                                + new String(fam,temp_cell.getFamilyOffset(),temp_cell.getFamilyLength()) +" "
                                + new String(fam_q,temp_cell.getQualifierOffset(),temp_cell.getQualifierLength())+" "
                                + new String(value,temp_cell.getValueOffset(),temp_cell.getValueLength())
                );
            }
            flag+=1;
        }
        System.out.println("Quantity of rows: "+ flag);
    }

    public static void insertData(String tableName,String rowKey,String colFamily,String col,String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(),col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
    }

    public static void getData(String tableName,String rowKey,String colFamily, String col)throws  IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(),col.getBytes());
        Result result = table.get(get);
        System.out.println(new String(result.getValue(colFamily.getBytes(),col==null?null:col.getBytes())));
        table.close();
    }

    public static void delData(String tableName, String colFamily,String qu,String rowKey)throws  IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete de = new Delete(Bytes.toBytes(rowKey));
        de.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(qu));
        table.delete(de);
    }

    public static void deltable(String tableName)throws  IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        int flag=0;
        HTableDescriptor temp = table.getTableDescriptor();
        String[] temp_args = new String[temp.getColumnFamilyNames().size()];
        for(byte[] tem:temp.getColumnFamilyNames()){
            System.out.println(Bytes.toString(tem));
            temp_args[flag] = Bytes.toString(tem);
            flag+=1;
        }

        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        createTable(tableName,temp_args);
    }
}