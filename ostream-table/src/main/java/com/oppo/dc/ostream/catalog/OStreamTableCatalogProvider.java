package com.oppo.dc.ostream.catalog;

import com.oppo.dc.ostream.OSteramTableConfig;
import com.oppo.dc.ostream.domain.OStreamDatabase;
import com.oppo.dc.ostream.repository.OStreamDatabaseRepository;
import com.oppo.dc.ostream.repository.OStreamTableRepository;
import com.uber.athenax.vm.api.tables.AthenaXTableCatalog;
import com.uber.athenax.vm.api.tables.AthenaXTableCatalogProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}
