package com.oppo.dc.ostream.repository;

import com.oppo.dc.ostream.domain.OStreamJob;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OStreamJobRepository extends JpaRepository<OStreamJob, String>{
    List<OStreamJob> findByName(String jobName);
}
