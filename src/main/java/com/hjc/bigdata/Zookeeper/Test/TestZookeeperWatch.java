package com.hjc.bigdata.Zookeeper.Test;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Description : TODO
 * @author : hjc
 * @date : 2021/5/18; 20:16
 * @Version : 1.0
 */
public class TestZookeeperWatch {

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
    public void lsAndWatch() throws Exception {
        zooKeeper.getChildren("/ideaZK", new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                System.out.println(event.getPath() + "发生了以下事件： " +event.getType());

                List<String> children;

                try {
                    children = zooKeeper.getChildren("/ideaZK", null);
                    System.out.println(event.getPath() + "新的节点：" + children);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        while (true) {
            Thread.sleep(5000);

            System.out.println("我还活着。。。。。。");
        }

    }

    private CountDownLatch cdl  =new CountDownLatch(1);

    @Test
    public void getAndWatch () throws Exception {

        //zooKeeper = new ZooKeeper(connectString, sessionTimeout, true)  会自动调用Before里面的process
        //是Connect线程调用
        byte[] data = zooKeeper.getData("/data2", new Watcher() {

            // 是Listener线程调用
            @Override
            public void process(WatchedEvent event) {

                System.out.println(event.getPath()+"发生了以下事件:"+event.getType());

                //减一
                cdl.countDown();
            }
        }, null);

        System.out.println("查询到的数据是:"+new String(data));

        //阻塞当前线程，当初始化的值变为0时，当前线程会唤醒
        cdl.await();
    }

    //持续watch
    @Test
    public void lsAndAlwaysWatch() throws Exception {

        //传入true,默认使用客户端自带的观察者
        zooKeeper.getChildren("/data2",new Watcher() {

            // process由listener线程调用，listener线程不能阻塞,阻塞后无法再调用process
            //当前线程自己设置的观察者
            @Override
            public void process(WatchedEvent event) {

                System.out.println(event.getPath()+"发生了以下事件:"+event.getType());

                System.out.println(Thread.currentThread().getName()+"---->我还活着......");

                try {
                    lsAndAlwaysWatch();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });

        //客户端所在的进程不能死亡
        while(true) {

            Thread.sleep(5000);

            System.out.println(Thread.currentThread().getName()+"---->我还活着......");

        }

    }

    @Test
    public void testLsAndAlwaysWatchCurrent() throws Exception {

        lsAndAlwaysWatchCurrent();

        //客户端所在的进程不能死亡
        while(true) {

            Thread.sleep(5000);

            System.out.println(Thread.currentThread().getName()+"---->我还活着......");

        }

    }

    @Test
    public void lsAndAlwaysWatchCurrent() throws Exception {

        //传入true,默认使用客户端自带的观察者
        zooKeeper.getChildren("/data2",new Watcher() {

            // process由listener线程调用，listener线程不能阻塞,阻塞后无法再调用process
            //当前线程自己设置的观察者
            @Override
            public void process(WatchedEvent event) {

                System.out.println(event.getPath()+"发生了以下事件:"+event.getType());

                System.out.println(Thread.currentThread().getName()+"---->我还活着......");

                try {
                    //递归调用
                    lsAndAlwaysWatchCurrent();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
    }

}
