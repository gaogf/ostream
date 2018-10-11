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
public class OStreamMetadataFeeds implements OStreamMetadata{
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

        // kafka configs
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "bj2569:9094,bj2583:9094,bj2584:9094,bj2658:9094,bj2660:9094");
        kafkaProps.put("group.id", "dc_demo_group");
        kafkaProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required\n" +
                "username=\"admin\"\n" +
                "password=\"3d#hS68315Xm\";");
        kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
        kafkaProps.put("sasl.mechanism", "PLAIN");
        kafkaProps.put("fetch.message.max.bytes", "5242880");
        kafkaProps.put("fetch.max.bytes", "262144000");

        OStreamTable sourceTable = new OStreamTable();
        sourceTable.setName("sdk_log_browser_feeds");
        sourceTable.setComment("浏览器信息流数据");
        sourceTable.setCreatedBy("80189083");
        sourceTable.setCreateTime(new Timestamp(System.currentTimeMillis()));
        sourceTable.setConnectorType(TableConnector.KAFKA);
        sourceTable.setFormatType(TableFormat.AVRO);
        sourceTable.setDatabase(database);

        // initialize table descriptors
        ConnectorDescriptor connectorDescriptor = new Kafka()
                .version("0.10")
                .topic("sdk_log_browser_feeds")
                .properties(kafkaProps)
                .startFromGroupOffsets();

        final String SOURCE_AVRO_SCHEMA = "    {\n" +
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
        FormatDescriptor formatDescriptor = new Avro().avroSchema(SOURCE_AVRO_SCHEMA);
        Schema schemaDesc = new Schema().schema(TableSchema.fromTypeInfo(
                AvroSchemaConverter.convertToTypeInfo(SOURCE_AVRO_SCHEMA)));

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

        // init sink tables
        OStreamTable sinkTable = new OStreamTable();
        sinkTable.setName("sdk_log_browser_feeds_test");
        sinkTable.setComment("浏览器信息流数据处理结果");
        sinkTable.setCreatedBy("80189083");
        sinkTable.setCreateTime(new Timestamp(System.currentTimeMillis()));
        sinkTable.setConnectorType(TableConnector.KAFKA);
        sinkTable.setFormatType(TableFormat.AVRO);
        sinkTable.setDatabase(database);

        // initialize table descriptors
        ConnectorDescriptor connectorDescriptor = new Kafka()
                .version("0.10")
                .topic("druid_browser_feeds_test")
                .properties(kafkaProps)
                .startFromGroupOffsets();

        final String AVRO_SCHEMA = "{\n" +
                "         \"namespace\": \"com.oppo.dc.data.avro.generated\",\n" +
                "         \"type\": \"record\",\n" +
                "         \"name\": \"SdkLogTest\",\n" +
                "         \"fields\": [\n" +
                "             {\"name\": \"imei\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"model\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"os_version\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"app_version\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"act_code\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"server_time\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"module\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"iflow_source\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"eventTag\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"stat_name\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"channel_name\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"from_id\", \"type\": [\"null\", \"string\"]},\n" +
                "             {\"name\": \"view_time\", \"type\": [\"null\", \"long\"]}\n" +
                "          ]\n" +
                "    }";
        FormatDescriptor formatDescriptor = new Json().schema(AvroSchemaConverter.convertToTypeInfo(AVRO_SCHEMA));
        Schema schemaDesc = new Schema().schema(TableSchema.fromTypeInfo(
                AvroSchemaConverter.convertToTypeInfo(AVRO_SCHEMA)));

        sinkTable.setConnectorParams(DescriptorProperties.toJavaMap(connectorDescriptor));
        sinkTable.setFormatParams(DescriptorProperties.toJavaMap(formatDescriptor));
        sinkTable.setSchemaParams(DescriptorProperties.toJavaMap(schemaDesc));

        tableRepository.save(sinkTable);
    }


    @Override
    public void initJobs(ApplicationContext ctx) {
        OStreamJobRepository jobRepository = ctx.getBean(OStreamJobRepository.class);

        OStreamJob job = OStreamJob.Builder.anOStreamJob()
                .withId(UUID.randomUUID().toString())
                .withName("feeds_job")
                .withCreatedBy("80189083")
                .withCreatTime(new Timestamp(System.currentTimeMillis()))
                .withCluster("bi-cluster")
                .withQuery("INSERT INTO `dw.sdk_log_browser_feeds_test` " +
                        "SELECT imei,model,os_version,app_version,event_id,server_time," +
                        "event_info['module'],event_info['iflow_source'],event_info['eventTag']," +
                        "event_info['stat_name'],event_info['channel_name'],event_info['from_id']," +
                        "CAST(event_info['view_time'] AS BIGINT) FROM dw.sdk_log_browser_feeds")
                .withOutput("")
                .withQueue("root.etlstream")
                .withVcores(48L)
                .withMemory(2048L)
                .withExecutionSlots(1L)
                .build();

        jobRepository.save(job);
    }
}
