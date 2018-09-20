package com.oppo.dc.ostream.domain;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name = "ostream_job")
public class OStreamJob {
    @Id
    @Column(name = "job_id")
    private String id;

    @Column(name = "job_name")
    private String name;

    @Column(name = "comment")
    private String comment;

    @Column(name = "create_time")
    private Timestamp creatTime;

    @Column(name = "create_by")
    private String createdBy;

    @Column(name = "sql_query")
    private String query;

    @Column(name = "sql_output")
    private String output;

    @Column(name = "yarn_cluster")
    private String cluster;

    @Column(name = "yarn_queue")
    private String queue;

    @Column(name = "yarn_memory")
    private Long memory;

    @Column(name = "yarn_vcores")
    private Long vcores;

    @Column(name = "execution_slots")
    private Long executionSlots;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Timestamp getCreatTime() {
        return creatTime;
    }

    public void setCreatTime(Timestamp creatTime) {
        this.creatTime = creatTime;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public Long getMemory() {
        return memory;
    }

    public void setMemory(Long memory) {
        this.memory = memory;
    }

    public Long getVcores() {
        return vcores;
    }

    public void setVcores(Long vcores) {
        this.vcores = vcores;
    }

    public Long getExecutionSlots() {
        return executionSlots;
    }

    public void setExecutionSlots(Long executionSlots) {
        this.executionSlots = executionSlots;
    }


    public static final class Builder {
        private String id;
        private String name;
        private String comment;
        private Timestamp creatTime;
        private String createdBy;
        private String query;
        private String output;
        private String cluster;
        private String queue;
        private Long memory;
        private Long vcores;
        private Long executionSlots;

        private Builder() {
        }

        public static Builder anOStreamJob() {
            return new Builder();
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder withCreatTime(Timestamp creatTime) {
            this.creatTime = creatTime;
            return this;
        }

        public Builder withCreatedBy(String createdBy) {
            this.createdBy = createdBy;
            return this;
        }

        public Builder withQuery(String query) {
            this.query = query;
            return this;
        }

        public Builder withOutput(String output) {
            this.output = output;
            return this;
        }

        public Builder withCluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        public Builder withQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder withMemory(Long memory) {
            this.memory = memory;
            return this;
        }

        public Builder withVcores(Long vcores) {
            this.vcores = vcores;
            return this;
        }

        public Builder withExecutionSlots(Long executionSlots) {
            this.executionSlots = executionSlots;
            return this;
        }

        public OStreamJob build() {
            OStreamJob oStreamJob = new OStreamJob();
            oStreamJob.setId(id);
            oStreamJob.setName(name);
            oStreamJob.setComment(comment);
            oStreamJob.setCreatTime(creatTime);
            oStreamJob.setCreatedBy(createdBy);
            oStreamJob.setQuery(query);
            oStreamJob.setOutput(output);
            oStreamJob.setCluster(cluster);
            oStreamJob.setQueue(queue);
            oStreamJob.setMemory(memory);
            oStreamJob.setVcores(vcores);
            oStreamJob.setExecutionSlots(executionSlots);
            return oStreamJob;
        }
    }
}
