package com.hjc.bigdata.Zookeeper.practice;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/21; 21:13
 * @Version : 1.0
 */
/*
    从Zookeeper集群获取当前启动的Server进程有哪些，获取到Server进程的信息，并持续监听Server进程的变化，一旦发生变化，重新获取Server进程的信息
 */
public class Router {

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
        Router router = new Router();

        router.init();

        router.check();

        router.getData();

        router.doOtherBusiness();
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

    //获取当前启动的Server进程有哪些，获取到Server进程的信息
    private List<String> getData() throws Exception{

        ArrayList<String> results = new ArrayList<>();

        List<String> children = zk.getChildren(basePath, new Watcher() {

            //递归，持续监听
            @Override
            public void process(WatchedEvent event) {

                System.out.println(event.getPath() + "发生了以下事件:" + event.getType());

                try {
                    getData();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });

        for (String child : children) {
            byte[] data = zk.getData(basePath + "/" + child, null, null);

            results.add(new String (data));
        }

        System.out.println("最新读取的信息：" + results);

        return results;
    }

    //检查/Servers是否存在，如果不存在，需要创建这个节点
    private void check() throws InterruptedException, KeeperException {

        Stat stat = zk.exists(basePath, false);

        if (stat == null) {
            //Server必须是永久节点
            zk.create(basePath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

    }

}
