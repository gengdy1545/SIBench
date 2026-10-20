package org.sibench.replay;

import org.sibench.capture.IndexOperationTrace;
import org.sibench.capture.IndexOperationTrace.Operation;

import java.io.IOException;
import java.util.List;

/** Calls return after execution; ordered groups do not imply atomic transactions. */
public interface ReplayTarget {
    void accept(Operation operation) throws IOException;

    void acceptGroup(List<Operation> operations) throws IOException;

    static ReplayTarget ordered(IndexOperationTrace.Consumer consumer) {
        return new ReplayTarget() {
            @Override
            public void accept(Operation operation) throws IOException {
                consumer.accept(operation);
            }

            @Override
            public void acceptGroup(List<Operation> operations) throws IOException {
                for (Operation operation : operations) consumer.accept(operation);
            }
        };
    }
}
