package com.oppo.dc.ostream.init;

import com.oppo.dc.ostream.domain.*;
import com.oppo.dc.ostream.repository.OStreamDatabaseRepository;
import com.oppo.dc.ostream.repository.OStreamJobRepository;
import com.oppo.dc.ostream.repository.OStreamTableRepository;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.descriptors.*;
import org.springframework.context.ApplicationContext;

import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

//@Component
public class OStreamMetadataLocal implements OStreamMetadata {
    public OStreamDatabase createOrGetDatabase(ApplicationContext ctx) {
        OStreamDatabaseRepository databaseRepository =
                ctx.getBean(OStreamDatabaseRepository.class);
        final String dbName = "db1";

        List<OStreamDatabase> dbs = databaseRepository.findByName(dbName);
        if(dbs.size() > 0) {
            return dbs.get(0);
        }

        OStreamDatabase database = new OStreamDatabase();
        database.setName(dbName);
        database.setComment("数据仓库");
        database.setCreateTime(new Timestamp(System.currentTimeMillis()));
        database.setCreatedBy("80189083");
        return databaseRepository.save(database);
    }

    @Override
    public void initSourceTables(ApplicationContext ctx) {
        OStreamDatabase database = createOrGetDatabase(ctx);

        OStreamTableRepository tableRepository =
                ctx.getBean(OStreamTableRepository.class);

        OStreamTable table = new OStreamTable();
        table.setName("tb1");
        table.setComment("comment");
        table.setConnectorType(TableConnector.KAFKA);
        table.setFormatType(TableFormat.JSON);
        table.setDatabase(database);

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("group.id", "jerryConsumer");

        // initialize table descriptors
        ConnectorDescriptor connectorDescriptor = new Kafka()
                .version("0.10")
                .topic("inputJerry")
                .properties(kafkaProps)
                .startFromEarliest();

        TableSchema schema = new TableSchemaBuilder()
                .field("id", Types.INT())
                .field("name", Types.STRING())
                .build();
        FormatDescriptor formatDescriptor = new Json().schema(schema.toRowType());
        Schema schemaDesc = new Schema().schema(schema);

        table.setConnectorParams(DescriptorProperties.toJavaMap(connectorDescriptor));
        table.setFormatParams(DescriptorProperties.toJavaMap(formatDescriptor));
        table.setSchemaParams(DescriptorProperties.toJavaMap(schemaDesc));

        tableRepository.save(table);
    }

    @Override
    public void initSinkTables(ApplicationContext ctx) {
        OStreamDatabase database = createOrGetDatabase(ctx);

        OStreamTableRepository tableRepository =
                ctx.getBean(OStreamTableRepository.class);

        OStreamTable table2 = new OStreamTable();
        table2.setName("tb2");
        table2.setComment("comment");
        table2.setConnectorType(TableConnector.KAFKA);
        table2.setFormatType(TableFormat.JSON);
        table2.setDatabase(database);

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");

        // initialize table descriptors
        ConnectorDescriptor connectorDescriptor2 = new Kafka()
                .version("0.10")
                .topic("outputJerry")
                .properties(kafkaProps)
                .startFromEarliest();

        TableSchema schema2 = new TableSchemaBuilder()
                .field("id", Types.INT())
                .field("name", Types.STRING())
                .build();
        FormatDescriptor formatDescriptor2 = new Json().schema(schema2.toRowType());
        Schema schemaDesc2 = new Schema().schema(schema2);

        table2.setConnectorParams(DescriptorProperties.toJavaMap(connectorDescriptor2));
        table2.setFormatParams(DescriptorProperties.toJavaMap(formatDescriptor2));
        table2.setSchemaParams(DescriptorProperties.toJavaMap(schemaDesc2));

        tableRepository.save(table2);
    }


    @Override
    public void initJobs(ApplicationContext ctx) {
        OStreamJobRepository jobRepository = ctx.getBean(OStreamJobRepository.class);

        OStreamJob job = OStreamJob.Builder.anOStreamJob()
                .withId(UUID.randomUUID().toString())
                .withName("demo_job")
                .withCluster("foo")
                .withQuery("INSERT INTO `db1.tb2` SELECT * FROM db1.tb1")
                .withOutput("")
                .withQueue("default")
                .withVcores(1L)
                .withMemory(1024L)
                .withExecutionSlots(1L)
                .build();

        jobRepository.save(job);
    }
}
