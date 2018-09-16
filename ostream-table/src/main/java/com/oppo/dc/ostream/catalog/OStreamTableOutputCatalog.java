package com.oppo.dc.ostream.catalog;

import com.oppo.dc.ostream.domain.OStreamTable;
import com.oppo.dc.ostream.descriptors.ConnectorDescriptorFactory;
import com.oppo.dc.ostream.descriptors.FormatDescriptorFactory;
import com.oppo.dc.ostream.repository.OStreamTableRepository;
import com.uber.athenax.vm.api.tables.AthenaXTableCatalog;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.factories.TableFactoryService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class OStreamTableOutputCatalog implements AthenaXTableCatalog, Serializable {
    private OStreamTableRepository tableRepository;
    private List<String> tables;

    public OStreamTableOutputCatalog(OStreamTableRepository tableRepository, List<String> tables) {
        this.tables = tables;
        this.tableRepository = tableRepository;
    }

    @Override
    public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
        if (!tableName.contains(".")) {
            throw new TableNotExistException("default", tableName);
        }

        String[] items = tableName.split("\\.", 2);
        String dbName = items[0];
        String table = items[1];

        List<OStreamTable> otables =  tableRepository.findByDatabase_NameAndName(dbName, table);
        if(otables.size() == 0) {
            throw new TableNotExistException(dbName, table);
        }

        OStreamTable otable = otables.get(0);

        // build ExternalCatalogTable based on descriptor parameters
        ConnectorDescriptor connectorDescriptor = TableFactoryService.find(
                ConnectorDescriptorFactory.class, otable.getConnectorParams())
                .createConnectorDescriptor(otable.getConnectorParams());
        FormatDescriptor formatDescriptor = TableFactoryService.find(
                FormatDescriptorFactory.class, otable.getFormatParams())
                .createFormatDescriptor(otable.getFormatParams());

        DescriptorProperties properties = new DescriptorProperties(true);
        properties.putProperties(otable.getSchemaParams());
        Schema schema = new Schema().schema(SchemaValidator.deriveFormatFields(properties));

        return ExternalCatalogTable.builder(connectorDescriptor).inAppendMode()
                .withSchema(schema)
                .withFormat(formatDescriptor)
                .asTableSourceAndSink();
    }

    @Override
    public List<String> listTables() {
        return tables;
    }

    @Override
    public ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException {
        throw new CatalogNotExistException(dbName);
    }

    @Override
    public List<String> listSubCatalogs() {
        return new ArrayList<>();
    }
}
