package org.sibench.capture;

import java.io.IOException;
import org.sibench.trace.IndexKey;

/** Encodes typed native Value::ToSQLString tokens without executing captured SQL. */
public final class DuckDbKeys {
    private DuckDbKeys() {}

    public static IndexKey parse(String type,String token) throws IOException {
        if(type==null||token==null) throw new IOException("Missing native key type or token");
        String family=family(type);
        if(token.equals("NULL::"+type)) return IndexKey.NULL;
        try {
            if(family.equals("int")) {
                if(!token.matches("-?(0|[1-9][0-9]*)")||token.equals("-0")) throw new IOException("Noncanonical native integer");
                long value=switch(type) {
                    case "TINYINT" -> Byte.parseByte(token);
                    case "SMALLINT" -> Short.parseShort(token);
                    case "INTEGER" -> Integer.parseInt(token);
                    default -> Long.parseLong(token);
                };
                return IndexKey.ofInt(value);
            }
            if(family.equals("varchar")) return IndexKey.ofText(text(token));
            String suffix="::"+type;
            if(!token.endsWith(suffix)) throw new IOException("Missing native temporal cast");
            String value=text(token.substring(0,token.length()-suffix.length()));
            return IndexKey.parse(type.equals("DATE")&&!value.endsWith("infinity")?"date":"timestamp",value);
        } catch(IllegalArgumentException failure) {throw new IOException("Invalid native "+type+" key",failure);}
    }

    public static String family(String type) throws IOException {
        if(type==null)throw new IOException("Missing native index key type");
        return switch(type) {
            case "TINYINT","SMALLINT","INTEGER","BIGINT" -> "int";
            case "VARCHAR" -> "varchar";
            case "TIMESTAMP","DATE" -> "timestamp";
            default -> throw new IOException("Unsupported native index key type: "+type);
        };
    }

    private static String text(String token) throws IOException {
        if(token.length()<2||token.charAt(0)!='\''||token.charAt(token.length()-1)!='\'')throw new IOException("Invalid native quoted key");
        var text=new StringBuilder();
        for(int i=1;i<token.length()-1;i++) {
            char c=token.charAt(i);
            if(c=='\''&&(++i>=token.length()-1||token.charAt(i)!='\''))throw new IOException("Invalid native quote escape");
            text.append(c);
        }
        return text.toString();
    }
}
