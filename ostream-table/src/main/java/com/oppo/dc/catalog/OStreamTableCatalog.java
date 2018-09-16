package com.oppo.dc.catalog;

import com.oppo.dc.domain.OStreamTable;
import com.oppo.dc.ostream.descriptors.ConnectorDescriptorFactory;
import com.oppo.dc.ostream.descriptors.FormatDescriptorFactory;
import com.oppo.dc.repository.OStreamTableRepository;
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
import java.util.stream.Collectors;

public class OStreamTableCatalog implements AthenaXTableCatalog, Serializable{
    private OStreamTableRepository tableRepository;
    private String dbName;

    public OStreamTableCatalog(OStreamTableRepository tableRepository, String dbName) {
        this.tableRepository = tableRepository;
        this.dbName = dbName;
    }

    @Override
    public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
        List<OStreamTable> otables = tableRepository.findByDatabase_NameAndName(dbName, tableName);
        if(otables.size() == 0) {
            throw new TableNotExistException(dbName, tableName);
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
        return tableRepository.findByDatabase_Name(dbName)
                .stream().map(t -> t.getName()).collect(Collectors.toList());
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
