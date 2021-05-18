package com.hjc.bigdata.Zookeeper.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/18; 20:41
 * @Version : 1.0
 */

/*
 * 1. 从ZK集群获取当前启动的Server进程有哪些，获取到Server进程的信息
 * 			持续监听Server进程的变化，一旦有变化，重新获取Server进程的信息
 */
public class Routor {

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

    //检查/Server是否存在，如果不存在，需要创建这个节点
    public void check () throws KeeperException, InterruptedException {

        Stat stat = zooKeeper.exists(basePath, false);

        //如果不存在， 初始化根节点
        if (stat ==null) {
            //Server必须是永久节点
            zooKeeper.create(basePath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    //获取当前启动的Server进程有哪些，获取到Server进程的信息
    public List<String> getData () throws KeeperException, InterruptedException {

        ArrayList<String> results = new ArrayList<String>();

        List<String> children = zooKeeper.getChildren(basePath, new Watcher() {

            //递归，持续监听
            @Override
            public void process(WatchedEvent event) {

                System.out.println(event.getPath() + "发生了以下事件:" + event.getType());

                try {
                    getData();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });

        //获取每个节点中保存的Server的信息
        for (String child : children) {

            byte[] info = zooKeeper.getData(basePath + "/" + child, null, null);

            results.add(new String(info));
        }

        System.out.println("最新读取到的信息： " + results);

        return results;

    }

    //其他的业务功能
    public void doOtherBusiness () throws InterruptedException {

        System.out.println("Working...........");

        //持续工作
        while (true) {
            Thread.sleep(5000);

            System.out.println("工作中。。。。。。。。。。。");
        }
    }

    public static void main(String[] args) throws Exception {

        Routor routor = new Routor();

        //初始化客户端
        routor.init();

        //检查节点状态
        routor.check();

        //获取数据
        routor.getData();

        //其他的工作
        routor.doOtherBusiness();
    }
}
