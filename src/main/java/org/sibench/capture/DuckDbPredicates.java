package org.sibench.capture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.sibench.trace.IndexKey;

/** Converts adopted ART probe conditions, never the original SQL WHERE or LIMIT. */
public final class DuckDbPredicates {
    public record Predicate(String comparison,String type,String valueSql) {}
    public record Probe(String origin,List<Predicate> predicates) {
        public Probe {predicates=List.copyOf(predicates);}
    }
    private DuckDbPredicates() {}

    public static List<IndexOperationTrace.Range> ranges(String family,List<Probe> probes) throws IOException {
        var ranges=new ArrayList<IndexOperationTrace.Range>();List<Predicate> base=null;
        for(var probe:probes) {
            if(probe.origin().equals("base")) {
                base=probe.predicates();ranges.add(range(family,base));
            } else if(!List.of("deleted_rows","checkpoint_added").contains(probe.origin())||base==null||!base.equals(probe.predicates())) {
                throw new IOException("Delta ART predicate differs from its base probe");
            }
        }
        if(ranges.isEmpty()) throw new IOException("Missing base ART probe");
        if(ranges.size()==1)return List.copyOf(ranges);
        // Native OR probes union candidates; the service currently expresses such unions as point groups.
        var points=new TreeSet<IndexKey>();
        for(var range:ranges) {
            if(range.lower()==null||!range.lower().equals(range.upper())||!range.lowerInclusive()||!range.upperInclusive())
                throw new IOException("Unsupported union of non-point ART ranges");
            points.add(range.lower());
        }
        return points.stream().map(IndexOperationTrace.Range::equal).toList();
    }

    private static IndexOperationTrace.Range range(String family,List<Predicate> predicates) throws IOException {
        if(predicates.isEmpty()||predicates.size()>2)throw new IOException("Invalid ART predicate count");
        IndexKey lower=null,upper=null;boolean lowerInclusive=false,upperInclusive=false;
        for(var predicate:predicates) {
            String actual=DuckDbKeys.family(predicate.type());
            if(!actual.equals(family)&&!(family.equals("text")&&actual.equals("varchar")))
                throw new IOException("ART predicate type differs from canonical index");
            IndexKey key=DuckDbKeys.parse(predicate.type(),predicate.valueSql());
            if(key.equals(IndexKey.NULL))throw new IOException("Unsupported NULL ART predicate");
            boolean low,high,inclusive;
            switch(predicate.comparison()) {
                case "EQUAL" -> {low=true;high=true;inclusive=true;}
                case "GREATERTHAN" -> {low=true;high=false;inclusive=false;}
                case "GREATERTHANOREQUALTO" -> {low=true;high=false;inclusive=true;}
                case "LESSTHAN" -> {low=false;high=true;inclusive=false;}
                case "LESSTHANOREQUALTO" -> {low=false;high=true;inclusive=true;}
                default -> throw new IOException("Unsupported ART comparison: "+predicate.comparison());
            }
            if(low&&(lower==null||key.compareTo(lower)>0)) {lower=key;lowerInclusive=inclusive;}
            else if(low&&key.equals(lower))lowerInclusive&=inclusive;
            if(high&&(upper==null||key.compareTo(upper)<0)) {upper=key;upperInclusive=inclusive;}
            else if(high&&key.equals(upper))upperInclusive&=inclusive;
        }
        if(lower!=null&&upper!=null&&(lower.compareTo(upper)>0||lower.equals(upper)&&(!lowerInclusive||!upperInclusive)))
            throw new IOException("Contradictory adopted ART bounds");
        return new IndexOperationTrace.Range(lower,lowerInclusive,upper,upperInclusive,false,0);
    }
}
