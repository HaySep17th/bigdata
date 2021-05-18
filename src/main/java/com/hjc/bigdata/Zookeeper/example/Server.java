package com.hjc.bigdata.Zookeeper.example;

import org.apache.zookeeper.*;
import org.junit.Before;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/18; 20:41
 * @Version : 1.0
 */
public class Server {

    private String connectString = "hp1:2181,hp2:2181";
    private int sessionTimeOut = 6000;
    private ZooKeeper zooKeeper;

    private String basePath = "/ideaZK";

    @Before
    public void init () throws Exception {

        //创建一个zookeeper客户端对象
        zooKeeper = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {

            //回调方法，一旦watcher观察的path触发了指定的事件，服务端会通知客户端，客户端收到通知后
            // 会自动调用process()
            @Override
            public void process(WatchedEvent event) {

            }
        });

        System.out.println(zooKeeper);
    }

    //使用zookeeper注册临时节点
    public void regist (String info) throws KeeperException, InterruptedException {

        //节点必须是临时带序号的节点
        zooKeeper.create(basePath + "/server", info.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

    }

    //其他业务功能
    public void doOtherBusiness () throws InterruptedException {
        System.out.println("Working...........");

        //持续工作
        while (true) {
            Thread.sleep(5000);

            System.out.println("工作中。。。。。。。。。。");
        }
    }

    public static void main(String[] args) throws Exception {

        Server server = new Server();

        server.init();

        server.regist("惠俊程学习Zookeeper的第一天。");

        server.doOtherBusiness();
    }

}
