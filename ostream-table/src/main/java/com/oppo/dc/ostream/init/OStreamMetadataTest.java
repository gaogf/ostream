package com.oppo.dc.ostream.init;

import com.oppo.dc.ostream.domain.*;
import com.oppo.dc.ostream.repository.OStreamDatabaseRepository;
import com.oppo.dc.ostream.repository.OStreamJobRepository;
import com.oppo.dc.ostream.repository.OStreamTableRepository;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.*;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

//@Component
public class OStreamMetadataTest implements OStreamMetadata{
    public OStreamDatabase createOrGetDatabase(ApplicationContext ctx) {
        OStreamDatabaseRepository databaseRepository =
                ctx.getBean(OStreamDatabaseRepository.class);
        final String dbName = "dw";

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
        table.setName("app_install_event");
        table.setComment("应用安装卸载事件");
        table.setCreatedBy("80189083");
        table.setCreateTime(new Timestamp(System.currentTimeMillis()));
        table.setConnectorType(TableConnector.KAFKA);
        table.setFormatType(TableFormat.AVRO);
        table.setDatabase(database);

        // kafka configs
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "bj2569:9094,bj2583:9094,bj2584:9094,bj2658:9094,bj2660:9094");
        kafkaProps.put("group.id", "dc_demo_group");
        kafkaProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required\n" +
                "username=\"admin\"\n" +
                "password=\"3d#hS68315Xm\";");
        kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
        kafkaProps.put("sasl.mechanism", "PLAIN");

        // initialize table descriptors
        ConnectorDescriptor connectorDescriptor = new Kafka()
                .version("0.10")
                .topic("app_install_event")
                .properties(kafkaProps)
                .startFromGroupOffsets();

        final String AVRO_SCHEMA = "    {\n" +
                "         \"namespace\": \"com.oppo.dc.data.avro.generated\",\n" +
                "         \"type\": \"record\",\n" +
                "         \"name\": \"AppInstallEvent\",\n" +
                "         \"fields\": [\n" +
                "             {\"name\": \"imei\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"app_id\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"action\", \"type\": [\"null\", \"int\"]}\n" +
                "          ]\n" +
                "    }";
        FormatDescriptor formatDescriptor = new Avro().avroSchema(AVRO_SCHEMA);
        Schema schemaDesc = new Schema().schema(TableSchema.fromTypeInfo(
                AvroSchemaConverter.convertToTypeInfo(AVRO_SCHEMA)));

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

        OStreamTable table = new OStreamTable();
        table.setName("app_install_event_output");
        table.setComment("测试Demo");
        table.setCreatedBy("80189083");
        table.setCreateTime(new Timestamp(System.currentTimeMillis()));
        table.setConnectorType(TableConnector.KAFKA);
        table.setFormatType(TableFormat.AVRO);
        table.setDatabase(database);

        // kafka configs
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "bj2569:9094,bj2583:9094,bj2584:9094,bj2658:9094,bj2660:9094");
        kafkaProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required\n" +
                "username=\"admin\"\n" +
                "password=\"3d#hS68315Xm\";");
        kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
        kafkaProps.put("sasl.mechanism", "PLAIN");

        // initialize table descriptors
        ConnectorDescriptor connectorDescriptor = new Kafka()
                .version("0.10")
                .topic("app_install_event_output")
                .properties(kafkaProps)
                .startFromGroupOffsets();

        final String AVRO_SCHEMA = "    {\n" +
                "         \"namespace\": \"com.oppo.dc.data.avro.generated\",\n" +
                "         \"type\": \"record\",\n" +
                "         \"name\": \"AppInstallEvent\",\n" +
                "         \"fields\": [\n" +
                "             {\"name\": \"imei\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"app_id\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"action\", \"type\": [\"null\", \"int\"]}\n" +
                "          ]\n" +
                "    }";
        FormatDescriptor formatDescriptor = new Avro().avroSchema(AVRO_SCHEMA);
        Schema schemaDesc = new Schema().schema(TableSchema.fromTypeInfo(
                AvroSchemaConverter.convertToTypeInfo(AVRO_SCHEMA)));

        table.setConnectorParams(DescriptorProperties.toJavaMap(connectorDescriptor));
        table.setFormatParams(DescriptorProperties.toJavaMap(formatDescriptor));
        table.setSchemaParams(DescriptorProperties.toJavaMap(schemaDesc));

        tableRepository.save(table);
    }


    @Override
    public void initJobs(ApplicationContext ctx) {
        OStreamJobRepository jobRepository = ctx.getBean(OStreamJobRepository.class);

        OStreamJob job = OStreamJob.Builder.anOStreamJob()
                .withId(UUID.randomUUID().toString())
                .withName("demo_job")
                .withCreatedBy("80189083")
                .withCreatTime(new Timestamp(System.currentTimeMillis()))
                .withCluster("bi-cluster")
                .withQuery("insert into `dw.app_install_event_output` select * from dw.app_install_event")
                .withOutput("")
                .withQueue("root.etlstream")
                .withVcores(4L)
                .withMemory(1024L)
                .withExecutionSlots(1L)
                .build();

        jobRepository.save(job);
    }
}
