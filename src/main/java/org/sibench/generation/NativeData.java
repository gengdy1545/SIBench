package org.sibench.generation;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

/** Row consumer contract shared by the explicitly selected native engines. */
public interface NativeData extends AutoCloseable {
    Optional<Map<String,Object>> snapshotRow(int table,long rowId,long readVersion)throws SQLException;
    Optional<Map<String,Object>> rowBeforeStatement(int table,long rowId,long connection,long query)throws SQLException;
    @Override void close()throws SQLException;
}
