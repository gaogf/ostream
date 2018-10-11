package com.oppo.dc.ostream.descriptors;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvDescriptorFactory implements FormatDescriptorFactory {
    @Override
    public FormatDescriptor createFormatDescriptor(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        TableSchema tableSchema = descriptorProperties.getTableSchema(CsvValidator.FORMAT_FIELDS());
        Csv formatDesc = new Csv().schema(tableSchema);

        return formatDesc;
    }

    @Override
    public Map<String, String> requiredContext() {
        final Map<String, String> context = new HashMap<>();
        context.put(FormatDescriptorValidator.FORMAT_TYPE(), CsvValidator.FORMAT_TYPE_VALUE());
        context.put(FormatDescriptorValidator.FORMAT_PROPERTY_VERSION(), "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        final List<String> properties = new ArrayList<>();
        properties.addAll(SchemaValidator.getSchemaDerivationKeys());
        properties.add("format.fields.#.name");
        properties.add("format.fields.#.type");
        properties.add("format.field-delimiter");
        return properties;
    }
}
