package com.hyf.zookeeper;



import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;


import java.util.concurrent.CountDownLatch;

/**
 * @author : heyanfeng
 * create at:  2019-09-13  22:32
 * @description: zk 服务观察者
 */
public class MyZkWatcher implements Watcher {
    private static Logger log= LogManager.getLogger(MyZkConnect.class);

    /**
    * description:异部锁
    * create by heyanfeng at 2019-09-13 22:35
    */
    private CountDownLatch cdl;


    /**
     * description:标记
     * create by heyanfeng at 2019-09-13 22:35
     */
    private String mark;

    public MyZkWatcher(CountDownLatch cdl, String mark) {
        this.cdl = cdl;
        this.mark = mark;
    }

    @Override
    public void process(WatchedEvent event) {
        log.info(mark + "watcher监听事件:{}",event);
        cdl.countDown();
    }
}

