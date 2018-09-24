package com.oppo.dc.ostream;

import com.oppo.dc.ostream.domain.OStreamJob;
import com.oppo.dc.ostream.repository.OStreamJobRepository;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

import java.sql.Timestamp;
import java.util.UUID;

public class OStreamJobInitializer {
    public static void main(String [] args) throws Exception {
        ApplicationContext context = SpringApplication.run(OSteramTableConfig.class);
        //initJobs(context);
        initFeedsJobs(context);
    }

    private static void initJobs(ApplicationContext ctx) throws Exception {
        OStreamJobRepository jobRepository = ctx.getBean(OStreamJobRepository.class);

        OStreamJob job = OStreamJob.Builder.anOStreamJob()
                .withId(UUID.randomUUID().toString())
                .withName("demo_job")
                .withCreatedBy("80189083")
                .withCreatTime(new Timestamp(System.currentTimeMillis()))
                .withCluster("bi-cluster")
                .withQuery("select * from dw.app_install_event")
                .withOutput("dw.app_install_event_output")
                .withQueue("root.etlstream")
                .withVcores(4L)
                .withMemory(1024L)
                .withExecutionSlots(1L)
                .build();

        jobRepository.save(job);

    }

    private static void initFeedsJobs(ApplicationContext ctx) throws Exception {
        OStreamJobRepository jobRepository = ctx.getBean(OStreamJobRepository.class);

        OStreamJob job = OStreamJob.Builder.anOStreamJob()
                .withId(UUID.randomUUID().toString())
                .withName("feeds_job")
                .withCreatedBy("80189083")
                .withCreatTime(new Timestamp(System.currentTimeMillis()))
                .withCluster("bi-cluster")
                .withQuery("select imei,model,os_version,app_version,event_id,server_time," +
                        "event_info['module'],event_info['iflow_source'],event_info['eventTag']," +
                        "event_info['stat_name'],event_info['channel_name'],event_info['from_id']," +
                        "event_info['view_time'] from dw.sdk_log_browser_feeds")
                .withOutput("dw.sdk_log_browser_feeds_test")
                .withQueue("root.etlstream")
                .withVcores(4L)
                .withMemory(1024L)
                .withExecutionSlots(1L)
                .build();

        jobRepository.save(job);

    }
}