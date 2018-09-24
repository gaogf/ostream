package com.oppo.dc.ostream;

import com.oppo.dc.ostream.domain.OStreamDatabase;
import com.oppo.dc.ostream.domain.OStreamTable;
import com.oppo.dc.ostream.domain.TableConnector;
import com.oppo.dc.ostream.domain.TableFormat;
import com.oppo.dc.ostream.repository.OStreamDatabaseRepository;
import com.oppo.dc.ostream.repository.OStreamTableRepository;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.*;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

import java.sql.Timestamp;
import java.util.Properties;

public class OStreamMetadataInitializer {
    public static void main(String [] args) throws Exception {
        ApplicationContext context = SpringApplication.run(OSteramTableConfig.class);
        OStreamDatabase db = initDatabase(context);
        initSinkTables(context, db);
        initSourceTables(context, db);
        initFeedsSourceTables(context, db);
        initFeedsSinkTables(context, db);
    }

    private static OStreamDatabase initDatabase(ApplicationContext ctx) throws Exception {
        OStreamDatabaseRepository databaseRepository =
                ctx.getBean(OStreamDatabaseRepository.class);

        OStreamDatabase database = new OStreamDatabase();
        database.setName("dw");
        database.setComment("数据仓库");
        database.setCreateTime(new Timestamp(System.currentTimeMillis()));
        database.setCreatedBy("80189083");
        return databaseRepository.save(database);
    }

    private static void initFeedsSourceTables(ApplicationContext ctx, OStreamDatabase database) {
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

    private static void initFeedsSinkTables(ApplicationContext ctx, OStreamDatabase database) {
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
                .topic("sdk_log_browser_feeds_test")
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
                "             {\"name\": \"view_time\", \"type\": [\"null\", \"string\"]}\n" +
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

    private static void initSourceTables(ApplicationContext ctx, OStreamDatabase database) throws Exception {
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

    private static void initSinkTables(ApplicationContext ctx, OStreamDatabase database) throws Exception {
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
}
