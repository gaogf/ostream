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

@Component
public class OStreamMetadataSdkLog implements OStreamMetadata{
    final String SOURCE_AVRO_SCHEMA = "{\n" +
            "         \"namespace\": \"com.oppo.dc.data.avro.generated\",\n" +
            "         \"type\": \"record\",\n" +
            "         \"name\": \"SdkLog\",\n" +
            "         \"fields\": [\n" +
            "             {\"name\": \"imei\", \"type\": [\"null\", \"string\"]},\n" +
            "             {\"name\": \"model\", \"type\": [\"null\", \"string\"]},\n" +
            "             {\"name\": \"os_version\", \"type\": [\"null\", \"string\"]},\n" +
            "             {\"name\": \"system_id\", \"type\": [\"null\", \"string\"]},\n" +
            "             {\"name\": \"ssoid\", \"type\": [\"null\", \"long\"]},\n" +
            "             {\"name\": \"app_version\", \"type\": [\"null\", \"string\"]},\n" +
            "             {\"name\": \"rom_version\", \"type\": [\"null\", \"string\"]},\n" +
            "             {\"name\": \"channel_id\", \"type\": [\"null\", \"int\"]},\n" +
            "             {\"name\": \"event_id\", \"type\": [\"null\", \"string\"]},\n" +
            "             {\"name\": \"duration\", \"type\": [\"null\", \"int\"]},\n" +
            "             {\"name\": \"event_time\", \"type\": [\"null\", \"long\"]},\n" +
            "             {\"name\": \"event_count\", \"type\": [\"null\", \"int\"]},\n" +
            "             {\"name\": \"client_ip\", \"type\": [\"null\", \"string\"]},\n" +
            "             {\"name\": \"network_type\", \"type\": [\"null\", \"string\"]},\n" +
            "             {\"name\": \"event_info\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}]},\n" +
            "             {\"name\": \"android_version\", \"type\": [\"null\", \"string\"]},\n" +
            "             {\"name\": \"sdk_version\", \"type\": [\"null\", \"string\"]},\n" +
            "             {\"name\": \"server_time\", \"type\": [\"null\", \"string\"]}\n" +
            "          ]\n" +
            "    }";

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

        // kafka related configs
        Properties srcKafkaProps = new Properties();
        srcKafkaProps.put("bootstrap.servers", "10.12.33.33:9092,10.12.33.42:9092,10.12.33.34:9092");
        srcKafkaProps.put("group.id", "dc_etl_group");

        OStreamTable sourceTable = new OStreamTable();
        sourceTable.setName("sdk_log");
        sourceTable.setComment("埋点上报sdk_log");
        sourceTable.setCreatedBy("80189083");
        sourceTable.setCreateTime(new Timestamp(System.currentTimeMillis()));
        sourceTable.setConnectorType(TableConnector.KAFKA);
        sourceTable.setFormatType(TableFormat.CSV);
        sourceTable.setDatabase(database);

        // initialize table descriptors
        ConnectorDescriptor connectorDescriptor = new Kafka()
                .version("0.10")
                .topic("sdk_log_s")
                .properties(srcKafkaProps)
                .startFromGroupOffsets();

        TableSchema tableSchema = TableSchema.fromTypeInfo(
                AvroSchemaConverter.convertToTypeInfo(SOURCE_AVRO_SCHEMA));
        FormatDescriptor formatDescriptor = new Csv().schema(tableSchema);
        Schema schemaDesc = new Schema().schema(tableSchema);

        sourceTable.setConnectorParams(DescriptorProperties.toJavaMap(connectorDescriptor));
        sourceTable.setFormatParams(DescriptorProperties.toJavaMap(formatDescriptor));
        sourceTable.setSchemaParams(DescriptorProperties.toJavaMap(schemaDesc));

        tableRepository.save(sourceTable);
    }

    @Override
    public void initSinkTables(ApplicationContext ctx) {
        OStreamDatabase database = createOrGetDatabase(ctx);

        OStreamTableRepository tableRepository =
                ctx.getBean(OStreamTableRepository.class);

        // kafka configs
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "bj2569:9094,bj2583:9094,bj2584:9094,bj2658:9094,bj2660:9094");
        kafkaProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required\n" +
                "username=\"admin\"\n" +
                "password=\"3d#hS68315Xm\";");
        kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
        kafkaProps.put("sasl.mechanism", "PLAIN");
        kafkaProps.put("request.timeout.ms", "1200000");
        kafkaProps.put("batch.size", "131072");


        FormatDescriptor formatDescriptor = new Avro().avroSchema(SOURCE_AVRO_SCHEMA);
        Schema schemaDesc = new Schema().schema(TableSchema.fromTypeInfo(
                AvroSchemaConverter.convertToTypeInfo(SOURCE_AVRO_SCHEMA)));


        initSinkTableInternal(database, tableRepository, "sdk_log_cdo", "内容分发数据",
                kafkaProps, formatDescriptor, schemaDesc);

        initSinkTableInternal(database, tableRepository, "sdk_log_browser_client", "浏览器客户端数据",
                kafkaProps, formatDescriptor, schemaDesc);

        initSinkTableInternal(database, tableRepository, "sdk_log_browser_feeds", "信息流数据",
                kafkaProps, formatDescriptor, schemaDesc);

        initSinkTableInternal(database, tableRepository, "sdk_log_browser_url", "浏览器访问数据",
                kafkaProps, formatDescriptor, schemaDesc);

        initSinkTableInternal(database, tableRepository, "sdk_log_browser_search", "浏览器搜索数据",
                kafkaProps, formatDescriptor, schemaDesc);

        initSinkTableInternal(database, tableRepository, "sdk_log_browser_others", "浏览器其他数据",
                kafkaProps, formatDescriptor, schemaDesc);
    }

    private void initSinkTableInternal(OStreamDatabase database, OStreamTableRepository tableRepository,
                                               String name, String comment, Properties kafkaProps,
                                               FormatDescriptor formatDescriptor, Schema schemaDesc) {
        OStreamTable sinkTable = new OStreamTable();
        sinkTable.setName(name);
        sinkTable.setComment(comment);
        sinkTable.setCreatedBy("80189083");
        sinkTable.setCreateTime(new Timestamp(System.currentTimeMillis()));
        sinkTable.setConnectorType(TableConnector.KAFKA);
        sinkTable.setFormatType(TableFormat.AVRO);
        sinkTable.setDatabase(database);

        ConnectorDescriptor connectorDescriptor = new Kafka()
                .version("0.10")
                .topic(name)
                .properties(kafkaProps)
                .startFromGroupOffsets();

        sinkTable.setConnectorParams(DescriptorProperties.toJavaMap(connectorDescriptor));
        sinkTable.setFormatParams(DescriptorProperties.toJavaMap(formatDescriptor));
        sinkTable.setSchemaParams(DescriptorProperties.toJavaMap(schemaDesc));

        tableRepository.save(sinkTable);
    }


    @Override
    public void initJobs(ApplicationContext ctx) {
        OStreamJobRepository jobRepository = ctx.getBean(OStreamJobRepository.class);

        String [] queries = new String[] {
            "INSERT INTO `dw.sdk_log_cdo` SELECT * FROM dw.sdk_log WHERE system_id = '2' OR system_id = '1000'",
            "INSERT INTO `dw.sdk_log_browser_client` SELECT * FROM dw.sdk_log WHERE system_id = '2007' AND event_info['eventTag'] = '10001'",
            "INSERT INTO `dw.sdk_log_browser_feeds` SELECT * FROM dw.sdk_log WHERE system_id = '2007' AND event_info['eventTag'] = '10012'",
            "INSERT INTO `dw.sdk_log_browser_url` SELECT * FROM dw.sdk_log WHERE system_id = '2007' AND event_info['eventTag'] = '10004'",
            "INSERT INTO `dw.sdk_log_browser_search` SELECT * FROM dw.sdk_log WHERE system_id = '2007' AND event_info['eventTag'] = '10006'",
            "INSERT INTO `dw.sdk_log_browser_others` SELECT * FROM dw.sdk_log WHERE system_id = '2007' AND event_info['eventTag'] not in ('10001', '10012', '10004', '10006')"
        };



        OStreamJob job = OStreamJob.Builder.anOStreamJob()
                .withId(UUID.randomUUID().toString())
                .withName("sdk_log_job")
                .withCreatedBy("80189083")
                .withCreatTime(new Timestamp(System.currentTimeMillis()))
                .withCluster("bi-cluster")
                .withQuery(String.join(";", queries))
                .withOutput("")
                .withQueue("root.etlstream")
                .withVcores(80L)
                .withMemory(2048L)
                .withExecutionSlots(1L)
                .build();

        jobRepository.save(job);
    }
}
