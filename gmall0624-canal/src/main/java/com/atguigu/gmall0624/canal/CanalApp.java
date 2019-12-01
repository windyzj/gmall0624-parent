package com.atguigu.gmall0624.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalApp {


    public static void main(String[] args) {
        //1 建立 和canal-server 连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop1", 11111), "example", "", "");


        while(true){
            canalConnector.connect();
            //2 订阅 数据
            canalConnector.subscribe("*.*");
            //3 抓取 message   n 条sql
            Message message = canalConnector.get(100);

            //
            int size = message.getEntries().size();
            if(size==0){
                try {
                    System.out.println("没有数据，休息5秒");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                for (CanalEntry.Entry entry : message.getEntries()) {  //entry 代表一条sql执行结果
                    if(entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) { //只要数据相关处理

                        ByteString storeValue = entry.getStoreValue(); //storevalue序列化的数据集
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);  //反序列化
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList(); //行集
                        String tableName = entry.getHeader().getTableName();//表名
                        CanalEntry.EventType eventType = rowChange.getEventType();//操作类型  insert  update delete

                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                        canalHandler.handle();//业务处理

                    }
                }

            }

        }





    }
}
