package com.oppo.dc.catalog;

import com.uber.athenax.vm.api.tables.AthenaXTableCatalog;
import com.uber.athenax.vm.api.tables.AthenaXTableCatalogProvider;

import java.util.List;
import java.util.Map;

public class OStreamTableCatalogProvider implements AthenaXTableCatalogProvider {
    @Override
    public Map<String, AthenaXTableCatalog> getInputCatalog(String s) {
        return null;
    }

    @Override
    public AthenaXTableCatalog getOutputCatalog(String s, List<String> list) {
        return null;
    }
}
