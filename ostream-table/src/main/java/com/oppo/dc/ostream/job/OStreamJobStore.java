package com.oppo.dc.ostream.job;

import com.oppo.dc.ostream.OSteramTableConfig;
import com.oppo.dc.ostream.domain.OStreamJob;
import com.oppo.dc.ostream.repository.OStreamJobRepository;
import com.uber.athenax.backend.api.ExtendedJobDefinition;
import com.uber.athenax.backend.api.JobDefinition;
import com.uber.athenax.backend.server.AthenaXConfiguration;
import com.uber.athenax.backend.server.jobs.JobStore;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

public class OStreamJobStore implements JobStore {
    private OStreamJobRepository jobRepository;

    @Override
    public void open(AthenaXConfiguration athenaXConfiguration) throws IOException {
        ApplicationContext context = SpringApplication.run(OSteramTableConfig.class);
        jobRepository = context.getBean(OStreamJobRepository.class);
    }

    @Override
    public JobDefinition get(UUID uuid) throws IOException {
        try {
            return JobConverter.fromOStreamJob(
                    jobRepository.findById(uuid.toString()).get())
                    .getDefinition();
        } catch (NoSuchElementException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void updateJob(UUID uuid, JobDefinition jobDefinition) throws IOException {

    }

    @Override
    public void removeJob(UUID uuid) throws IOException {

    }

    @Override
    public List<ExtendedJobDefinition> listAll() throws IOException {
        return JobConverter.fromOStreamJobList(jobRepository.findAll());
    }

    @Override
    public void close() throws IOException {
        // nothing to do
    }

    public static void main(String [] args) throws Exception {
        OStreamJobStore jobStore = new OStreamJobStore();
        jobStore.open(null);

        OStreamJob job = OStreamJob.Builder.anOStreamJob()
                .withId(UUID.randomUUID().toString())
                .withName("demo_job")
                .withCluster("foo")
                .withQuery("select * from db1.tb1")
                .withOutput("db1.tb2")
                .withQueue("default")
                .withVcores(1L)
                .withMemory(1024L)
                .withExecutionSlots(1L)
                .build();

        jobStore.jobRepository.save(job);
    }
}
