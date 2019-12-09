package wiki.zzq;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import wiki.zzq.scheduler.MyScheduler;

public class Main {
    public static void main(String[] args) {

        String frameworkName = "mesos-framework-demo";
        String user = "root";
        String role = "*";
        String zkUrl = "zk://localhost:2181/mesos";
        double failoverTimeout = 0;

        SchedulerDriver driver = new MesosSchedulerDriver(
                // 自己实现的Scheduler
                new MyScheduler(),
                // 定义Mesos framework配置
                Protos.FrameworkInfo.newBuilder()
                        .setUser(user)
                        .setRole(role)
                        .setName(frameworkName)
                        .setFailoverTimeout(failoverTimeout)
                        .setCheckpoint(true)
                        // .setHostname() 设置Mesos页面上Host列的值
                        // .setWebuiUrl() 设置framework的web访问地址
                        .build(),
                // Mesos的zk地址，用于获取mesos master地址
                zkUrl
        );

        // 这里如果找不到meososlib会阻塞在这里，下个方法不走
        int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;
        System.out.println(status);
    }
}
