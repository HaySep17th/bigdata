package com.hjc.bigdata.Zookeeper.practice;

import org.apache.zookeeper.*;
import org.junit.Before;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/21; 21:03
 * @Version : 1.0
 */
/*
    1 每次启动后，在执行自己的核心业务之前，先向ZK集群注册一个临时节点，且向临时节点中保存一些关键信息
 */
public class Server {

    private String connectString = "hp1:2181,hp2:2181,hp3:2181";
    private int sessionTimeOut = 6000;
    private ZooKeeper zk;

    private String basePath = "/ideaZK";

    //初始化客户端对象
    @Before
    public void init () throws Exception {

        //  创建一个zookeeper客户端对象
        zk = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }

    public static void main(String[] args) throws Exception {

        Server server = new Server();

        server.init();

        server.regist(args[0]);

        server.otherBusiness();
    }

    private void otherBusiness() throws InterruptedException {

        System.out.println("working!");

        //持续工作
        while (true) {
            Thread.sleep(5000);

            System.out.println("持续工作中！");
        }
    }

    //注册临时节点
    private void regist(String info) throws InterruptedException, KeeperException {

        zk.create(basePath + "/server", info.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

    }

}
