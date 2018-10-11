package com.oppo.dc.ostream.init;

import org.springframework.context.ApplicationContext;

public interface OStreamMetadata {
    void initSourceTables(ApplicationContext ctx);
    void initSinkTables(ApplicationContext ctx);
    void initJobs(ApplicationContext ctx);
}
