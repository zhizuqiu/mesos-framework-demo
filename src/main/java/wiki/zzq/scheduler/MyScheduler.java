package wiki.zzq.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.Collections;
import java.util.List;

public class MyScheduler implements Scheduler {
    private static boolean hasRun = false;

    @Override
    public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        // framework注册成功后的回调
        System.out.println("framework registered:" + frameworkId.getValue());
    }

    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> list) {
        // 当资源offer到达时，会回调这个方法

        double cpus = 0.1;
        double mem = 128;
        String role = "*";
        String cmd = "echo hello world!";

        System.out.println("received offers:");
        for (Protos.Offer offer : list) {
            System.out.println(offer.getHostname());

            // 如果任务已经下发，拒绝掉offer
            if (hasRun) {
                schedulerDriver.declineOffer(offer.getId());
                continue;
            }

            if (!Tools.matchResources(offer, cpus, mem, role)) {
                System.out.println("not match resources!");
                // 不满足要求，就拒绝掉
                schedulerDriver.declineOffer(offer.getId());
            }

            // 告诉mesos要下发的任务配置，以及使用到的资源
            Protos.Status status = schedulerDriver.acceptOffers(
                    // 使用到的offer id
                    Collections.singletonList(offer.getId()),
                    // 定义操作类型为LAUNCH一个任务，并设置任务配置信息
                    Collections.singletonList(Protos.Offer.Operation.newBuilder().
                            setType(Protos.Offer.Operation.Type.LAUNCH).
                            setLaunch(Protos.Offer.Operation.Launch.newBuilder().addTaskInfos(Tools.buildTaskInfo(offer, cpus, mem, role, cmd)))
                            .build()),
                    // 设置接受时的Filters，这里为8秒
                    Protos.Filters.newBuilder().setRefuseSeconds(8).build()
            );
            System.out.println("acceptOffers status:" + status);

            if (status.equals(Protos.Status.DRIVER_RUNNING)) {
                hasRun = true;
            }
        }

        // 如果任务已经下发，则告诉Mesos不再需要offer
        if (hasRun) {
            System.out.println("suppressOffers");
            schedulerDriver.suppressOffers();
        }
    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        // 任务状态更新时回调此方法
        System.out.println(taskStatus.getTaskId().getValue() + ":" + taskStatus.getState());
    }

    @Override
    public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
        System.out.println("framework reregistered!");
    }

    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerId) {
        // offer失效时的回调，一般不用实现
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] bytes) {

    }

    @Override
    public void disconnected(SchedulerDriver schedulerDriver) {

    }

    @Override
    public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveId) {

    }

    @Override
    public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int i) {

    }

    @Override
    public void error(SchedulerDriver schedulerDriver, String s) {

    }
}
