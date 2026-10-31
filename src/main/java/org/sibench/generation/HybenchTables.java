package org.sibench.generation;

import static org.sibench.config.ConfigValues.*;

import com.moandjiezana.toml.Toml;

import org.apache.parquet.example.data.Group;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** Deterministic derivation of the pinned HyBench baseline, not native byte-for-byte output. */
final class HybenchTables {
    private final GenerationConfig config;
    private final Toml parameters;
    private final Map<String, String[]> cities = new LinkedHashMap<>();
    private final String[] female, male, last, provinces, categories, phones;
    private final long customers, companies, start, middle, end, loanStart;

    HybenchTables(GenerationConfig config) throws IOException {
        this.config = config;
        Path source = config.root().resolve(string(object(config.document(), "source"), "path"));
        Path resources = source.resolve("src/main/resources");
        String sf = string(object(config.document(), "native"), "sf");
        parameters = new Toml().read(resources.resolve("parameters.toml").toFile()).getTable(sf);
        require(parameters != null, "Unknown HyBench sf: " + sf);
        customers = parameters.getLong("customer_number");
        companies = parameters.getLong("company_number");
        female = lines(resources.resolve("FirstName_Female"));
        male = lines(resources.resolve("FirstName_Male"));
        last = lines(resources.resolve("LastName"));
        Map<String, List<String>> locations = new LinkedHashMap<>();
        for (String line : lines(resources.resolve("CityByProvince"))) {
            String[] parts = line.split("\\s+");
            require(parts.length == 2, "Invalid CityByProvince row");
            locations.computeIfAbsent(parts[0], ignored -> new ArrayList<>()).add(parts[1]);
        }
        locations.forEach(
                (province, values) -> cities.put(province, values.toArray(String[]::new)));
        String randomSource =
                Files.readString(
                        source.resolve("src/main/java/com/hybench/util/RandomGenerator.java"));
        provinces = stringArray(randomSource, "Provinces_municipalities");
        categories = stringArray(randomSource, "CompanyCategories");
        phones = stringArray(randomSource, "prefix_3");
        for (String province : provinces)
            require(cities.containsKey(province), "Missing city list: " + province);
        start = date("startDate");
        middle = date("midPointDate");
        end = date("endDate");
        loanStart = date("LoanDate");
    }

    private static String[] lines(Path file) throws IOException {
        return Files.readAllLines(file).stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);
    }

    private static String[] stringArray(String source, String name) {
        var matcher =
                Pattern.compile("\\b" + name + "\\s*=\\s*\\{([^}]+)}", Pattern.DOTALL)
                        .matcher(source);
        require(matcher.find(), "Cannot locate pinned vocabulary: " + name);
        var tokens = Pattern.compile("\"([^\"]*)\"").matcher(matcher.group(1));
        List<String> values = new ArrayList<>();
        while (tokens.find()) values.add(tokens.group(1));
        require(!values.isEmpty(), "Empty vocabulary: " + name);
        return values.toArray(String[]::new);
    }

    private long date(String name) {
        return LocalDate.parse(parameters.getString(name))
                .atStartOfDay()
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();
    }

    List<TableData> describe() {
        List<TableData> tables = new ArrayList<>();
        long next = 1;
        String time = "optional int64 timestamp (TIMESTAMP(MILLIS,true)); ";
        String credits =
                "required int64 saving_credit; required int64 checking_credit; required int64"
                    + " loan_credit; required int32 isblocked; ";
        String dates =
                "required int64 created_date (TIMESTAMP(MILLIS,true)); required int64"
                    + " last_update_timestamp (TIMESTAMP(MILLIS,true)); ";
        tables.add(
                SourceTables.table(
                        config,
                        "customer",
                        customers,
                        next,
                        "custid",
                        "required int64 custid; required int64 companyid; required binary gender"
                            + " (UTF8); required binary name (UTF8); required int32 age; required"
                            + " binary phone (UTF8); required binary province (UTF8); required"
                            + " binary city (UTF8); required double loan_balance; "
                                + credits
                                + dates,
                        (i, g) -> customer(i + 1, g)));
        next += customers;
        tables.add(
                SourceTables.table(
                        config,
                        "company",
                        companies,
                        next,
                        "companyid",
                        "required int64 companyid; required binary name (UTF8); required binary"
                            + " category (UTF8); required int32 staff_size; required double"
                            + " loan_balance; required binary phone (UTF8); required binary"
                            + " province (UTF8); required binary city (UTF8); "
                                + credits
                                + dates,
                        (i, g) -> company(customers + i + 1, g)));
        next += companies;
        for (String table : List.of("savingaccount", "checkingaccount")) {
            tables.add(
                    SourceTables.table(
                            config,
                            table,
                            customers + companies,
                            next,
                            "accountid",
                            "required int64 accountid; required int64 userid; required double"
                                + " balance; required int32 isblocked; "
                                    + time
                                    + (table.equals("savingaccount")
                                            ? "optional int64 fresh_ts (TIMESTAMP(MILLIS,true));"
                                            : ""),
                            (i, g) -> account(table, i + 1, g)));
            next += customers + companies;
        }
        for (String table : List.of("transfer", "checking")) {
            long count = parameters.getLong(table + "_number");
            tables.add(
                    SourceTables.table(
                            config,
                            table,
                            count,
                            next,
                            "id",
                            "required int64 id; required int64 sourceid; required int64 targetid;"
                                + " required double amount; required binary type (UTF8); "
                                    + time
                                    + (table.equals("transfer")
                                            ? "optional int64 fresh_ts (TIMESTAMP(MILLIS,true));"
                                            : ""),
                            (i, g) -> transfer(table, count, i + 1, g)));
            next += count;
        }
        long loans = parameters.getLong("loanapps_number");
        for (String table : List.of("loanapps", "loantrans")) {
            tables.add(
                    SourceTables.table(
                            config,
                            table,
                            loans,
                            next,
                            "id",
                            "required int64 id; required int64 applicantid; required double amount;"
                                + " required int32 duration; required binary status (UTF8); "
                                    + time
                                    + (table.equals("loantrans")
                                            ? "required int64 appid; optional int64"
                                                  + " contract_timestamp (TIMESTAMP(MILLIS,true));"
                                                  + " required int32 delinquency;"
                                            : ""),
                            (i, g) -> loan(table, loans, i + 1, g)));
            next += loans;
        }
        return List.copyOf(tables);
    }

    private DeterministicRandom random(String domain, long id) {
        return new DeterministicRandom(config.seed(), "hybench/" + domain, id);
    }

    private int blocked(long id) {
        return random("blocked", id).fraction() < parameters.getDouble("prob_blocked") ? 1 : 0;
    }

    private long created(long id) {
        return random("created", id).between(start, middle);
    }

    private void location(Group g, DeterministicRandom r) {
        String province = r.pick(provinces);
        g.add("province", province);
        g.add("city", r.pick(cities.get(province)));
        StringBuilder phone = new StringBuilder(r.pick(phones));
        for (int i = 0; i < 8; i++) phone.append(r.between(0, 10));
        g.add("phone", phone.toString());
    }

    private void credits(Group g, long id) {
        g.add("saving_credit", 0L);
        g.add("checking_credit", 0L);
        g.add("loan_credit", 0L);
        g.add("isblocked", blocked(id));
        g.add("created_date", created(id));
        g.add("last_update_timestamp", created(id));
    }

    private Group customer(long id, Group g) {
        var r = random("customer", id);
        String gender = r.pick(new String[] {"female", "male"});
        g.add("custid", id);
        g.add("companyid", r.between(customers + 1, customers + companies + 1));
        g.add("gender", gender);
        g.add("name", r.pick(last) + " " + r.pick(gender.equals("female") ? female : male));
        g.add(
                "age",
                (int)
                        r.between(
                                parameters.getLong("customer_age_lower"),
                                parameters.getLong("customer_age_upper")));
        g.add("loan_balance", parameters.getLong("customer_loanbalance").doubleValue());
        location(g, r);
        credits(g, id);
        return g;
    }

    private Group company(long id, Group g) {
        var r = random("company", id);
        String category = r.pick(categories);
        g.add("companyid", id);
        g.add("category", category);
        g.add("name", category + id + category.hashCode());
        g.add(
                "staff_size",
                (int)
                        r.between(
                                parameters.getLong("company_size_lower"),
                                parameters.getLong("company_size_upper")));
        g.add("loan_balance", parameters.getLong("company_loanbalance").doubleValue());
        location(g, r);
        credits(g, id);
        return g;
    }

    private Group account(String table, long id, Group g) {
        String balance =
                (id <= customers ? "customer_" : "company_")
                        + (table.equals("savingaccount") ? "savingbalance" : "checkingbalance");
        g.add("accountid", id);
        g.add("userid", id);
        g.add("balance", parameters.getLong(balance).doubleValue());
        g.add("isblocked", blocked(id));
        g.add("timestamp", created(id));
        return g;
    }

    private Group transfer(String table, long count, long id, Group g) {
        var r = random(table, id);
        boolean transfer = table.equals("transfer");
        double rate = parameters.getDouble("cust_rate");
        boolean customer =
                id <= (transfer ? Math.round(count * rate) : (long) (count * (1 - rate)));
        long source =
                customer
                        ? r.between(1, customers + 1)
                        : r.between(customers + 1, customers + companies + 1);
        long target;
        do {
            target = r.between(1, customers + companies + 1);
        } while (target == source);
        String[] types =
                transfer
                        ? (customer
                                ? new String[] {"transfer", "red_packet", "donate", "invest"}
                                : new String[] {"business", "salary", "service", "invest"})
                        : (customer
                                ? new String[] {"transfer", "red_packet"}
                                : new String[] {"business", "salary", "service"});
        String balance =
                transfer
                        ? "customer_savingbalance"
                        : (customer ? "customer_checkingbalance" : "company_checkingbalance");
        g.add("id", id);
        g.add("sourceid", source);
        g.add("targetid", target);
        g.add("amount", Math.round(r.fraction() * parameters.getLong(balance) * .01 * 100) / 100.0);
        g.add("type", r.pick(types));
        g.add(
                "timestamp",
                r.between(
                        transfer && !customer ? start : middle,
                        transfer && !customer ? middle : end));
        return g;
    }

    private Group loan(String table, long count, long id, Group g) {
        var r = random("loan", id);
        boolean customer = id <= (long) (count * (1 - parameters.getDouble("cust_rate")));
        long applicant =
                customer
                        ? r.between(1, customers + 1)
                        : r.between(customers + 1, customers + companies + 1);
        double amount =
                Math.round(
                                r.fraction()
                                        * parameters.getLong(
                                                customer
                                                        ? "customer_loanbalance"
                                                        : "company_loanbalance")
                                        * parameters.getDouble("loan_rate")
                                        * 100)
                        / 100.0;
        long date = r.between(loanStart, end);
        int[] durations = {30, 60, 90, 180, 365};
        int duration = durations[(int) r.between(0, durations.length)];
        String status = r.pick(new String[] {"under_review", "reject", "accept"});
        g.add("id", id);
        g.add("applicantid", applicant);
        g.add("amount", amount);
        g.add("duration", duration);
        g.add("status", status);
        g.add("timestamp", table.equals("loanapps") ? date : date + 86400000L);
        if (table.equals("loantrans")) {
            g.add("appid", id);
            g.add("delinquency", 0);
            if (status.equals("accept")) g.add("contract_timestamp", date + 86400000L);
        }
        return g;
    }
}
