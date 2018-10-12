package com.oppo.dc.ostream.init;

import com.oppo.dc.ostream.OSteramTableConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OStreamMetadataInitializer {
    @Autowired(required=false)
    private List<OStreamMetadata> oStreamMetadataList;

    public static void main(String [] args) {
        ApplicationContext context = SpringApplication.run(OSteramTableConfig.class);
        OStreamMetadataInitializer initializer = context.getBean(OStreamMetadataInitializer.class);
        initializer.init(context);
    }

    public void init(ApplicationContext ctx) {
        if(oStreamMetadataList == null)return;

        for(OStreamMetadata metadata : oStreamMetadataList) {
            metadata.initSourceTables(ctx);
            metadata.initSinkTables(ctx);
            metadata.initJobs(ctx);
        }
    }
}
