package com.example.canalexample.service;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.example.canalexample.dao.UserRespository;
import com.example.canalexample.enums.OperaType;
import com.example.canalexample.po.UserPO;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class HandlerService implements ApplicationContextAware,InitializingBean {

    private static final Logger logger= LoggerFactory.getLogger(HandlerService.class);

    private ApplicationContext applicationContext;

    @Override
    public void afterPropertiesSet() throws Exception {
        syncDataToEs();
    }

    // 同步数据到ES
    private void syncDataToEs(){
        int batchSize = 100;
        CanalConnector cananlConnector=null;
        try {
            cananlConnector = CanalSyncService.createCanalConnect("172.19.177.120", 2181, "example");
            cananlConnector.connect();
            cananlConnector.subscribe("my_test.user");
            while (true) {
                Message message = cananlConnector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    logger.debug("没有获取到消息！");
                    cananlConnector.ack(batchId); // 提交确认
                    continue;
                }
                try{
                    logger.debug("message[batchId={}],size={}] \n", batchId, size);
                    syncDataToES(message.getEntries());
                    cananlConnector.ack(batchId); // 提交确认
                }catch (Exception e){
                    try{
                        cananlConnector.rollback(batchId);
                    }catch (Exception e1){
                        logger.info("数据回滚失败！batchId{}",batchId);
                        throw new Exception(e1);
                    }
                }
            }
        }catch (Exception e){
            if (Objects.nonNull(cananlConnector)){
                cananlConnector.disconnect();
            }
        }
    }

    /**
     * 抽取数据从canal中
     * @param entries
     */
    private void syncDataToES(List<CanalEntry.Entry> entries) throws InvalidProtocolBufferException {
        if(CollectionUtils.isEmpty(entries)){
            return;
        }
        for(CanalEntry.Entry entry :entries){
           if (Objects.nonNull(entry)){
               if (entry.getEntryType()== CanalEntry.EntryType.TRANSACTIONBEGIN ||
                       entry.getEntryType()== CanalEntry.EntryType.TRANSACTIONEND){
                   continue;
               }
               String logFileName= entry.getHeader().getLogfileName();
               long position=entry.getHeader().getLogfileOffset();
               String schema=entry.getHeader().getSchemaName();
               String table=entry.getHeader().getTableName();
               logger.info("消费到的数据信息为：binlog文件名 【{}】，点位 【{}】，schema名称 【{}】，表名 【{}】",logFileName,position,schema,table);
               CanalEntry.RowChange rowChange=CanalEntry.RowChange.parseFrom(entry.getStoreValue());
               if (rowChange.getIsDdl() || CollectionUtils.isEmpty(rowChange.getRowDatasList())){
                   continue;
               }
               logger.debug("数据库对应的sql为【{}】",rowChange.getSql());
               CanalEntry.EventType eventType=rowChange.getEventType();
               if (eventType== CanalEntry.EventType.INSERT || eventType== CanalEntry.EventType.UPDATE){
                   List<UserPO> userPOList= Lists.newArrayList();
                   rowChange.getRowDatasList().forEach(i->{
                       List<CanalEntry.Column> columnList=i.getAfterColumnsList();
                       if(!CollectionUtils.isEmpty(columnList)){
                           UserPO userPO= new UserPO();
                           columnList.forEach(column -> {
                               if (column.getName().equalsIgnoreCase("id")){
                                   userPO.setId(Integer.parseInt(column.getValue()));
                               }


                               if(column.getName().equalsIgnoreCase("name")){
                                   userPO.setName(column.getValue());
                               }
                               if(column.getName().equalsIgnoreCase("role_name")){
                                   userPO.setRoleName(column.getValue());
                               }
                               if(column.getName().equalsIgnoreCase("role_id")){
                                   userPO.setRoleId(Integer.parseInt(column.getValue()));
                               }
                           });
                           userPOList.add(userPO);
                       }
                   });
                   if(CollectionUtils.isEmpty(userPOList)){
                       continue;
                   }
//                   handlerTypeAndData.put(eventType== CanalEntry.EventType.CREATE?OperaType.ADD.name():OperaType.UPDATE.name(),userPOList);
                   writeToEs(OperaType.ADD,userPOList);
               }
               if (eventType== CanalEntry.EventType.DELETE){
                   List<UserPO> userPOList= Lists.newArrayList();
                   rowChange.getRowDatasList().forEach(i->{
                       List<CanalEntry.Column> columnList=i.getBeforeColumnsList();
                       if(!CollectionUtils.isEmpty(columnList)){
                           UserPO userPO= new UserPO();
                           columnList.forEach(column -> {
                               if (column.getName().equalsIgnoreCase("id")){
                                   userPO.setId(Integer.parseInt(column.getValue()));
                               }
                               if(column.getName().equalsIgnoreCase("name")){
                                   userPO.setName(column.getValue());
                               }
                               if(column.getName().equalsIgnoreCase("role_name")){
                                   userPO.setRoleName(column.getValue());
                               }
                               if(column.getName().equalsIgnoreCase("role_id")){
                                   userPO.setRoleId(Integer.parseInt(column.getValue()));
                               }
                           });
                           userPOList.add(userPO);
                       }
                   });
                   if(CollectionUtils.isEmpty(userPOList)){
                       continue;
                   }
//                   handlerTypeAndData.put(OperaType.DELETE.name(),userPOList);
                   writeToEs(OperaType.DELETE,userPOList);
               }
           }

        }
    }

    private void writeToEs(OperaType operaType,List<UserPO> datas){
        if(CollectionUtils.isEmpty(datas)){
            return;
        }
        if (operaType==OperaType.ADD || operaType==OperaType.UPDATE){
            applicationContext.getBean(UserRespository.class).saveAll(datas);
        }
        if(operaType==OperaType.DELETE){
            applicationContext.getBean(UserRespository.class).deleteAll(datas);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext=applicationContext;
    }
}
