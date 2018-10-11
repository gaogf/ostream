package com.oppo.dc.ostream.repository;

import com.oppo.dc.ostream.domain.OStreamDatabase;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OStreamDatabaseRepository extends JpaRepository<OStreamDatabase, Integer> {
    List<OStreamDatabase> findByName(String dbName);
}
