package com.oppo.dc.ostream.descriptors;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.table.descriptors.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroDescriptorFactory implements FormatDescriptorFactory {
    @Override
    public FormatDescriptor createFormatDescriptor(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        Avro formatDesc = new Avro();
        if (descriptorProperties.containsKey(AvroValidator.FORMAT_RECORD_CLASS)) {
            formatDesc.recordClass(descriptorProperties.getClass(
                    AvroValidator.FORMAT_RECORD_CLASS, SpecificRecord.class));
        } else {
            formatDesc.avroSchema(descriptorProperties.getString(
                    AvroValidator.FORMAT_AVRO_SCHEMA));
        }

        return formatDesc;
    }

    @Override
    public Map<String, String> requiredContext() {
        final Map<String, String> context = new HashMap<>();
        context.put(FormatDescriptorValidator.FORMAT_TYPE(), AvroValidator.FORMAT_TYPE_VALUE);
        context.put(FormatDescriptorValidator.FORMAT_PROPERTY_VERSION(), "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        final List<String> properties = new ArrayList<>();
        properties.add(AvroValidator.FORMAT_RECORD_CLASS);
        properties.add(AvroValidator.FORMAT_AVRO_SCHEMA);
        return properties;
    }
}
