package com.oppo.dc.ostream.job;

import com.oppo.dc.ostream.domain.OStreamJob;
import com.uber.athenax.backend.api.ExtendedJobDefinition;
import com.uber.athenax.backend.api.JobDefinition;
import com.uber.athenax.backend.api.JobDefinitionDesiredstate;
import com.uber.athenax.backend.api.JobDefinitionResource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class JobConverter {
    public static ExtendedJobDefinition fromOStreamJob(OStreamJob job) {
        JobDefinitionResource jobResource = new JobDefinitionResource()
                .queue(job.getQueue())
                .vCores(job.getVcores())
                .memory(job.getMemory())
                .executionSlots(job.getExecutionSlots());

        JobDefinitionDesiredstate desiredstate = new JobDefinitionDesiredstate()
                .clusterId(job.getCluster())
                .resource(jobResource);

        JobDefinition jobDefinition = new JobDefinition()
                .query(job.getQuery())
                .outputs(Arrays.asList(job.getOutput().split(",")))
                .addDesiredStateItem(desiredstate);

        return new ExtendedJobDefinition()
                        .uuid(UUID.fromString(job.getId()))
                        .definition(jobDefinition);

    }

    public static List<ExtendedJobDefinition> fromOStreamJobList(List<OStreamJob> jobs) {
        List<ExtendedJobDefinition> extendedJobDefinitions = new ArrayList<>();
        for (OStreamJob job : jobs) {
            extendedJobDefinitions.add(fromOStreamJob(job));
        }

        return extendedJobDefinitions;
    }
}
