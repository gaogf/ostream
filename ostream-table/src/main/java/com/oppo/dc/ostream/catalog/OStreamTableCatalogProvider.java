package com.oppo.dc.ostream.catalog;

import com.oppo.dc.ostream.OSteramTableConfig;
import com.oppo.dc.ostream.domain.OStreamDatabase;
import com.oppo.dc.ostream.domain.OStreamTable;
import com.oppo.dc.ostream.domain.TableConnector;
import com.oppo.dc.ostream.domain.TableFormat;
import com.oppo.dc.ostream.repository.OStreamDatabaseRepository;
import com.oppo.dc.ostream.repository.OStreamTableRepository;
import com.uber.athenax.vm.api.tables.AthenaXTableCatalog;
import com.uber.athenax.vm.api.tables.AthenaXTableCatalogProvider;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.descriptors.*;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OStreamTableCatalogProvider implements AthenaXTableCatalogProvider {
    private OStreamDatabaseRepository databaseRepository;
    private OStreamTableRepository tableRepository;

    public OStreamTableCatalogProvider() {
        ApplicationContext context = SpringApplication.run(OSteramTableConfig.class);
        databaseRepository = context.getBean(OStreamDatabaseRepository.class);
        tableRepository = context.getBean(OStreamTableRepository.class);
    }

    @Override
    public Map<String, AthenaXTableCatalog> getInputCatalog(String s) {
        List<OStreamDatabase> databases = databaseRepository.findAll();
        Map<String, AthenaXTableCatalog> catalogs = new HashMap<>();
        for (OStreamDatabase db : databases) {
            catalogs.put(db.getName(), new OStreamTableCatalog(tableRepository, db.getName()));
        }

        return catalogs;
    }

    @Override
    public AthenaXTableCatalog getOutputCatalog(String s, List<String> list) {
        return null;
    }

    public static void main(String [] args) {
        initTables();
    }

    public static void initTables() {
        OStreamTableCatalogProvider provider = new OStreamTableCatalogProvider();

        OStreamDatabase database = new OStreamDatabase();
        database.setName("db1");
        database.setComment("comment");
        provider.databaseRepository.save(database);

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

        provider.tableRepository.save(table);

        OStreamTable table2 = new OStreamTable();
        table2.setName("tb2");
        table2.setComment("comment");
        table2.setConnectorType(TableConnector.KAFKA);
        table2.setFormatType(TableFormat.JSON);
        table2.setDatabase(database);

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

        provider.tableRepository.save(table2);
    }
}
