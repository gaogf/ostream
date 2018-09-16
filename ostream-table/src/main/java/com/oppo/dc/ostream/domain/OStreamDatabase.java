package com.oppo.dc.ostream.domain;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name = "ostream_database")
public class OStreamDatabase {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    @Column(name = "database_id")
    private int id;

    @Column(name = "database_name")
    private String name;

    @Column(name = "create_by")
    private String createdBy;

    @Column(name = "create_time")
    private Timestamp createTime;

    @Column(name = "comment")
    private String comment;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}
