package com.oppo.dc.ostream.descriptors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.typeutils.TypeStringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonDescriptorFactory implements FormatDescriptorFactory {
    @Override
    public FormatDescriptor createFormatDescriptor(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        Json formatDesc = new Json();
        if (descriptorProperties.containsKey(JsonValidator.FORMAT_JSON_SCHEMA)) {
            formatDesc.jsonSchema(descriptorProperties.getString(
                    JsonValidator.FORMAT_JSON_SCHEMA));
        } else {
            TypeInformation typeInfo = TypeStringUtils.readTypeInfo(descriptorProperties.getString(
                    JsonValidator.FORMAT_SCHEMA));
            formatDesc.schema(typeInfo);
        }

        return formatDesc;
    }

    @Override
    public Map<String, String> requiredContext() {
        final Map<String, String> context = new HashMap<>();
        context.put(FormatDescriptorValidator.FORMAT_TYPE(), JsonValidator.FORMAT_TYPE_VALUE);
        context.put(FormatDescriptorValidator.FORMAT_PROPERTY_VERSION(), "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        final List<String> properties = new ArrayList<>();
        properties.add(JsonValidator.FORMAT_JSON_SCHEMA);
        properties.add(JsonValidator.FORMAT_SCHEMA);
        properties.add(JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD);
        properties.add(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA());
        properties.addAll(SchemaValidator.getSchemaDerivationKeys());
        return properties;
    }
}
