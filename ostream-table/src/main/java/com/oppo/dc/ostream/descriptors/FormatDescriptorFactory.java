package com.oppo.dc.ostream.descriptors;

import org.apache.flink.table.descriptors.FormatDescriptor;

import java.util.Map;

public interface FormatDescriptorFactory extends TableDescriptorFactory {
    FormatDescriptor createFormatDescriptor(Map<String, String> properties);
}
