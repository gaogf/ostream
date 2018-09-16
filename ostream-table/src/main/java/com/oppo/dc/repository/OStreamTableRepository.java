package com.oppo.dc.repository;

import com.oppo.dc.domain.OStreamTable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OStreamTableRepository extends JpaRepository<OStreamTable, Integer>{
    List<OStreamTable> findByDatabase_NameAndName(String dbName, String tableName);
    List<OStreamTable> findByDatabase_Name(String dbName);
}
