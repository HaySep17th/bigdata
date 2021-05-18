package com.hjc.bigdata.Zookeeper.Test;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/18; 20:16
 * @Version : 1.0
 */
public class TestZookeeper {

    private String connectString = "hp1:2181,hp2:2181";
    private int sessionTimeOut = 6000;
    private ZooKeeper zooKeeper;

    @Before
    public void init () throws Exception {

        //创建一个zookeeper客户端对象
        Watcher watcher;
        zooKeeper = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {

            //回调方法，一旦watcher观察的path触发了指定的事件，服务端会通知客户端，客户端收到通知后
            // 会自动调用process()
            @Override
            public void process(WatchedEvent event) {

            }
        });

        System.out.println(zooKeeper);
    }

    @After
    public void close () throws InterruptedException {

        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    @Test
    public void ls () throws Exception {

        Stat stat = new Stat();

        List<String> children = zooKeeper.getChildren("/ideaZK", null, stat);

        System.out.println(children);

        System.out.println(stat);
    }

    @Test
    public void create() throws Exception {
        zooKeeper.create("/ideaZK", "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void get() throws Exception {

        byte[] data = zooKeeper.getData("/ideaZK", null, null);
        System.out.println(new String(data) + "节点下的信息。。。");
    }

    // set path data
    @Test
    public void set() throws Exception {

        zooKeeper.setData("/ideaZK", "hi".getBytes(), -1);

    }

    // delete path
    @Test
    public void delete() throws Exception {

        zooKeeper.delete("/ideaZK", -1);

    }

    // rmr path
    @Test
    public void rmr() throws Exception {

        String path="/ideaZK";

        //先获取当前路径中所有的子node
        List<String> children = zooKeeper.getChildren(path, false);

        //删除所有的子节点
        for (String child : children) {

            zooKeeper.delete(path+"/"+child, -1);

        }

        zooKeeper.delete(path, -1);

    }

    // 判断当前节点是否存在
    @Test
    public void ifNodeExists() throws Exception {

        Stat stat = zooKeeper.exists("/ideaZK/server", false);

        System.out.println(stat==null ? "不存在" : "存在");

    }
}
