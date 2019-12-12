package com.hyf.zookeeper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author : heyanfeng
 * create at:  2019-09-13  22:08
 * @description: zookeeper 链接
 */
public class MyZkConnect {
    private static Logger log= LogManager.getLogger(MyZkConnect.class);

    public static final String ZK_SERVER_CLUSTER_CONNECT = "58.87.114.3:2181,58.87.114.3:2182,58.87.114.3:2183";

    public static final  String ZK_SERVER_SINGLE_CONNECT = "58.87.114.3:2181";

    public static final int TIMEOUT = 3000;


    public static void main(String[] args) throws  Exception{
        ZooKeeper zk = connect();
        log.info("zk 状态：{},zk-SessionId:{}",zk.getState(),zk.getSessionId());
//        queryStat(zk,"/");
//        ZooKeeper reZk = reconnect(zk.getSessionId(), zk.getSessionPasswd());
//        log.info("reZk 状态:{},zk-SessionId:{}",reZk.getState(),reZk.getSessionId());
        String nodePath = "/parent1";
        String nodeData = "father && monther";
        create(zk,"/parent","father && monther");
        Stat stat = queryStat(zk, nodePath);
        System.out.println("添加成功之后查询"+stat);
        String newNodeData = "son && duaghter";
        Stat updateStat = update(zk, nodePath, newNodeData);
        System.out.println("修改之后查询"+updateStat);

    }

    /**
    * description: 连接zk服务
    * create by heyanfeng at 2019-09-13 22:48
    * @Author heyanfeng
    * @return ZooKeeper
    */
    public static ZooKeeper connect() throws Exception{
        CountDownLatch cdl = new CountDownLatch(1);
        log.info("准备建立zk 服务");
        ZooKeeper zk = new ZooKeeper(ZK_SERVER_SINGLE_CONNECT, TIMEOUT, new MyZkWatcher(cdl, "建立链接"));
        log.info("完成建立zk 服务");
        cdl.await();
        return zk;
    }

    /**
    * description: 重新连接服务
    * create by heyanfeng at 2019-09-13 22:56
    * @return ZooKeeper
    * @param sessionId
    * @param sessionPasswd
    * 关闭后的会话连接，不支持重连。重连后，前会话连接将会失效
    */
    public static ZooKeeper reconnect(long sessionId, byte[] sessionPasswd) throws  Exception{
        CountDownLatch cdl = new CountDownLatch(1);
        log.info("准备重新连接zk服务");
        ZooKeeper zk = new ZooKeeper(ZK_SERVER_CLUSTER_CONNECT, TIMEOUT, new MyZkWatcher(cdl,"重新连接"), sessionId, sessionPasswd);
        log.info("完成重新连接zk服务");
        cdl.await();//这里为了等待wather监听事件结束
        return zk;

    }


    /**
    * description: 创建节点
    * create by heyanfeng at 2019-09-13 22:57
    * @param zk 节点
    * @param nodePath 节点数据
    * @param nodeData 节点数据
    */
    public static void create(ZooKeeper zk,String nodePath,String nodeData) throws Exception{
        CountDownLatch cdl = new CountDownLatch(1);
        log.info("开始创建节点:{},数据:{}",nodePath,nodeData);
        List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
        CreateMode createMode = CreateMode.PERSISTENT;
        String result = zk.create(nodePath, nodeData.getBytes(), acl, createMode);
        log.info("创建节点返回结果：{}",result);
        log.info("完成创建节点:{},数据:{}",nodePath,nodeData);

    }


    /**
     * 描述：查询节点结构信息
     * 作者：hyf
     * @param zk
     * @param nodePath
     * @return Stat
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static Stat queryStat(ZooKeeper zk, String nodePath) throws KeeperException, InterruptedException{
        return zk.exists(nodePath, false);
    }

    /**
    * description: 获取节点数据
    * create by heyanfeng at 2019-09-15 15:58
    * @return String
    * @param zk
    * @param nodePath
    */
    public static String queryData(ZooKeeper zk,String nodePath) throws Exception{
        return new String(zk.getData(nodePath, false, queryStat(zk, nodePath)));
    }



    /**
    * description: 更新节点数据
    * create by heyanfeng at 2019-09-15 16:01
    * @return Stat
    * @param zk
    * @param nodePath
    * @param nodeData
    */
     public static Stat update(ZooKeeper zk,String nodePath,String nodeData) throws Exception{
         Stat stat = queryStat(zk, nodePath);
         Stat newStat = new Stat();
         if(stat != null){
           newStat = zk.setData(nodePath, nodeData.getBytes(), stat.getVersion());
           return newStat;
         }else {
             log.info("修改的节点不存在");
         }
       return  null;
     }

     /**
     * description: 删除节点
     * create by heyanfeng at 2019-09-15 16:03
     * @param zk
     * @param nodePath
     */
     public static  void  delete(ZooKeeper zk,String nodePath) throws  Exception{
         Stat stat = queryStat(zk, nodePath);
         if(stat !=null){
             zk.delete(nodePath,stat.getVersion());
         }else {
             log.info("删除的节点不存在");
         }


     }
}
