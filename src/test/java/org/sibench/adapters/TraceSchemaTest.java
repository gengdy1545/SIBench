package org.sibench.adapters;

import static org.junit.Assert.*;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors;
import java.util.*;
import org.junit.Test;
import org.sibench.adapters.proto.TraceRecord;

/** The fixture is the descriptor of the persisted Trace before the data API migration. */
public class TraceSchemaTest {
    private static void messages(Map<String,DescriptorProto> types,String prefix,List<DescriptorProto> messages) {
        for(var message:messages) {
            String name=prefix+message.getName(); types.put(name,message);
            messages(types,name+".",message.getNestedTypeList());
        }
    }
    private static DescriptorProto binaryShape(DescriptorProto message) {
        var normalized=message.toBuilder();
        // Java-generated descriptors omit inferred JSON names; they do not affect Trace bytes.
        normalized.getFieldBuilderList().forEach(field -> field.clearJsonName());
        for(int i=0;i<normalized.getNestedTypeCount();i++)
            normalized.setNestedType(i,binaryShape(normalized.getNestedType(i)));
        return normalized.build();
    }
    @Test public void storedMessagesKeepEveryFieldNumberTypeOneofAndEnum() throws Exception {
        var before=new TreeMap<String,DescriptorProto>();
        try(var input=getClass().getResourceAsStream("/trace/index_trace_descriptor.pb")) {
            assertNotNull(input);
            for(var file:FileDescriptorSet.parseFrom(input).getFileList())
                messages(before,file.getPackage()+".",file.getMessageTypeList());
        }
        var pending=new ArrayDeque<Descriptors.Descriptor>(); pending.add(TraceRecord.getDescriptor());
        var checked=new HashSet<String>();
        while(!pending.isEmpty()) {
            var descriptor=pending.remove(); if(!checked.add(descriptor.getFullName())) continue;
            assertEquals(descriptor.getFullName(),binaryShape(before.get(descriptor.getFullName())),binaryShape(descriptor.toProto()));
            for(var field:descriptor.getFields()) if(field.getJavaType()==Descriptors.FieldDescriptor.JavaType.MESSAGE)
                pending.add(field.getMessageType());
        }
        assertEquals(12,checked.size());
    }
}
