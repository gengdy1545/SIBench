package org.sibench.adapters;

import org.sibench.adapters.proto.*;

/** Maps stored logical operations to the data API without changing the Trace format. */
final class IndexRpc {
    private IndexRpc() {}

    static PutRequest put(Write write) {
        if (write.getKind() != Write.Kind.INSERT && write.getKind() != Write.Kind.UPSERT)
            throw new IllegalArgumentException("Not a Put: " + write.getKind());
        return PutRequest.newBuilder().setKey(write.getKey()).setVersion(write.getVersion()).setValue(write.getValue()).build();
    }

    static DeleteRequest delete(Write write) {
        return DeleteRequest.newBuilder().setKey(write.getKey()).setVersion(write.getVersion()).build();
    }

    static Write write(PutRequest put) {
        var write = Write.newBuilder().setKind(Write.Kind.UPSERT).setKey(put.getKey()).setVersion(put.getVersion());
        if (put.hasValue()) write.setValue(put.getValue());
        if (!put.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("Unsupported Put fields");
        return write.build();
    }

    static Write write(DeleteRequest delete) {
        if (!delete.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("Unsupported Delete fields");
        return Write.newBuilder().setKind(Write.Kind.LOGICAL_DELETE).setKey(delete.getKey()).setVersion(delete.getVersion()).build();
    }

    static BatchWriteRequest batch(WriteBatch writes) {
        var batch = BatchWriteRequest.newBuilder();
        for (var write : writes.getWritesList()) {
            var mutation = Mutation.newBuilder();
            if (write.getKind() == Write.Kind.LOGICAL_DELETE) mutation.setDelete(delete(write));
            else mutation.setPut(put(write));
            batch.addMutations(mutation);
        }
        return batch.build();
    }

    static GetRequest get(Search search) {
        boolean equal = search.hasLower() && search.hasUpper() && search.getLower().getInclusive()
                && search.getUpper().getInclusive() && search.getLower().getKey().equals(search.getUpper().getKey());
        if (!search.getPinnedPrimaryKey().isEmpty() && (!equal || search.getIndex() == 0))
            throw new IllegalArgumentException("A pinned search names one secondary entry");
        if (!equal || search.getIndex() != 0 && search.getPinnedPrimaryKey().isEmpty()) return null;
        var key = EntryKey.newBuilder().setTable(search.getTable()).setIndex(search.getIndex());
        if (search.getIndex() == 0) key.setPrimaryKey(search.getLower().getKey());
        else key.setIndexedValue(search.getLower().getKey()).setPrimaryKey(search.getPinnedPrimaryKey());
        return GetRequest.newBuilder().setKey(key).setReadVersion(search.getReadVersion()).build();
    }

    static ScanRequest scan(Search search) {
        var scan = ScanRequest.newBuilder().setTable(search.getTable()).setIndex(search.getIndex())
                .setReadVersion(search.getReadVersion()).setDescending(search.getDescending()).setLimit(search.getLimit());
        if (search.hasLower()) scan.setLower(search.getLower());
        if (search.hasUpper()) scan.setUpper(search.getUpper());
        return scan.build();
    }

    static Search search(GetRequest get) {
        IndexExecution.validateKey(get.getKey());
        if (!get.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("Unsupported Get fields");
        var key = get.getKey();
        var bound = Bound.newBuilder().setInclusive(true).setKey(key.getIndex() == 0 ? key.getPrimaryKey() : key.getIndexedValue());
        var search = Search.newBuilder().setTable(key.getTable()).setIndex(key.getIndex())
                .setReadVersion(get.getReadVersion()).setLower(bound).setUpper(bound);
        if (key.getIndex() != 0) search.setPinnedPrimaryKey(key.getPrimaryKey());
        return search.build();
    }

    static Search search(ScanRequest scan) {
        if (!scan.getUnknownFields().asMap().isEmpty()) throw new IllegalArgumentException("Unsupported Scan fields");
        var search = Search.newBuilder().setTable(scan.getTable()).setIndex(scan.getIndex())
                .setReadVersion(scan.getReadVersion()).setDescending(scan.getDescending()).setLimit(scan.getLimit());
        if (scan.hasLower()) search.setLower(scan.getLower());
        if (scan.hasUpper()) search.setUpper(scan.getUpper());
        return search.build();
    }
}
