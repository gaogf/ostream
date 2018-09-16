package com.oppo.dc.ostream.domain;

import javax.persistence.*;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

@Entity
@Table(name = "ostream_table")
public class OStreamTable {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    @Column(name = "table_id")
    private int id;

    @Column(name = "table_name")
    private String name;

    @Column(name = "create_by")
    private String createdBy;

    @Column(name = "create_time")
    private Timestamp createTime;

    @Column(name = "comment")
    private String comment;

    @ManyToOne
    @JoinColumn(name="database_id")
    private OStreamDatabase database;

    @Column(name = "connector_type")
    @Enumerated(EnumType.STRING)
    private TableConnector connectorType;

    @Column(name = "format_type")
    @Enumerated(EnumType.STRING)
    private TableFormat formatType;

    @ElementCollection(fetch=FetchType.EAGER)
    @CollectionTable(name = "ostream_connector_params",
            joinColumns=@JoinColumn(name="table_id"))
    @MapKeyColumn(name = "`key`")
    @Column(name = "value", length = 10240)
    private Map<String, String> connectorParams = new HashMap<>();

    @ElementCollection(fetch=FetchType.EAGER)
    @CollectionTable(name = "ostream_format_params",
            joinColumns=@JoinColumn(name="table_id"))
    @MapKeyColumn(name = "`key`")
    @Column(name = "value", length = 10240)
    private Map<String, String> formatParams = new HashMap<>();

    @ElementCollection(fetch=FetchType.EAGER)
    @CollectionTable(name = "ostream_schema_params",
            joinColumns=@JoinColumn(name="table_id"))
    @MapKeyColumn(name = "`key`")
    @Column(name = "value", length = 10240)
    private Map<String, String> schemaParams = new HashMap<>();

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

    public OStreamDatabase getDatabase() {
        return database;
    }

    public void setDatabase(OStreamDatabase database) {
        this.database = database;
    }

    public TableConnector getConnectorType() {
        return connectorType;
    }

    public void setConnectorType(TableConnector connectorType) {
        this.connectorType = connectorType;
    }

    public TableFormat getFormatType() {
        return formatType;
    }

    public void setFormatType(TableFormat formatType) {
        this.formatType = formatType;
    }

    public Map<String, String> getConnectorParams() {
        return connectorParams;
    }

    public void setConnectorParams(Map<String, String> connectorParams) {
        this.connectorParams = connectorParams;
    }

    public Map<String, String> getFormatParams() {
        return formatParams;
    }

    public void setFormatParams(Map<String, String> formatParams) {
        this.formatParams = formatParams;
    }

    public Map<String, String> getSchemaParams() {
        return schemaParams;
    }

    public void setSchemaParams(Map<String, String> schemaParams) {
        this.schemaParams = schemaParams;
    }
}
