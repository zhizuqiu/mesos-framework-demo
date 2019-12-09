package wiki.zzq.scheduler;

import org.apache.mesos.Protos;

import java.util.Arrays;

public class Tools {
    // 生成一个测试任务
    public static Protos.TaskInfo buildTaskInfo(Protos.Offer offer, double cpus, double mem, String role, String cmd) {
        // 任务id
        String taskId = "task_id_1";
        // 任务名字
        String taskName = "task_name";

        Protos.TaskInfo.Builder builder = Protos.TaskInfo.newBuilder();
        builder.setName(taskName)
                .setTaskId(Protos.TaskID.newBuilder().setValue(taskId).build())
                .setSlaveId(offer.getSlaveId());

        builder.addAllResources(
                Arrays.asList(
                        // 所需的cpus
                        Protos.Resource.newBuilder()
                                .setName("cpus")
                                .setType(Protos.Value.Type.SCALAR)
                                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpus))
                                .setRole(role)
                                // .setReservation() 使用预留资源时
                                .build(),
                        // 所需的mem
                        Protos.Resource.newBuilder()
                                .setName("mem")
                                .setType(Protos.Value.Type.SCALAR)
                                .setScalar(Protos.Value.Scalar.newBuilder().setValue(mem))
                                .setRole(role)
                                // .setReservation() 使用预留资源时
                                .build()
                )
        );

        // builder.setLabels() 设置任务label，用于标识和mesos插件读取
        // builder.setDiscovery() 设置服务发现机制
        // builder.setHealthCheck() 设置健康检查

        builder.setCommand(Protos.CommandInfo.newBuilder().setShell(true).setValue(cmd));

        return builder.build();
    }

    public static boolean matchResources(Protos.Offer offer, double cpus, double mem, String role) {
        boolean matchedCpus = false;
        boolean matchedMem = false;
        for (Protos.Resource r : offer.getResourcesList()) {
            if (r.getRole().equals(role) && "cpus".equals(r.getName()) && r.getScalar().getValue() > cpus) {
                matchedCpus = true;
            }
            if (r.getRole().equals(role) && "mem".equals(r.getName()) && r.getScalar().getValue() > mem) {
                matchedMem = true;
            }
        }
        return matchedCpus && matchedMem;
    }
}
