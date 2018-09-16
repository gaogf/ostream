package com.oppo.dc.ostream.domain;

import com.oppo.dc.ostream.repository.OStreamDatabaseRepository;
import com.oppo.dc.ostream.repository.OStreamTableRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = OStreamTableTest.class,
        webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigurationPackage
@EnableJpaRepositories("**.repository")
@DataJpaTest
public class OStreamTableTest {
    @Autowired
    private OStreamTableRepository tableRepository;

    @Autowired
    private OStreamDatabaseRepository databaseRepository;

    @Test
    public void testSaveAndFind(){
        OStreamDatabase database = new OStreamDatabase();
        database.setName("db1");
        database.setComment("comment");
        databaseRepository.save(database);

        OStreamTable table = new OStreamTable();
        table.setName("table1");
        table.setComment("comment");
        table.setDatabase(database);

        tableRepository.save(table);

        OStreamDatabase database2 = new OStreamDatabase();
        database2.setName("db2");
        database2.setComment("comment");
        databaseRepository.save(database2);
        OStreamTable table2 = new OStreamTable();
        table2.setName("table2");
        table2.setComment("comment");
        table2.setDatabase(database2);
        table2.getConnectorParams().put("key", "name");
        table2.getConnectorParams().put("value", "jerryjzhang");

        tableRepository.save(table2);

        List<OStreamTable> tables = tableRepository.findByDatabase_NameAndName("db2", "table2");
        System.out.println(tables.get(0).getConnectorParams().get("key"));
    }
}
