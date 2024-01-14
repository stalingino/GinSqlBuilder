package st.gin.utils;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.util.StringUtils;
import st.gin.dto.SearchParam;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MySqlBuilder {
    public static final DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private StringBuilder count = new StringBuilder("SELECT count(*) FROM (SELECT ");
    private StringBuilder select = new StringBuilder("SELECT ");
    private StringBuilder where = new StringBuilder(" WHERE 1=1 ");
    private StringBuilder from = new StringBuilder(" FROM ");
    private StringBuilder orderBy = new StringBuilder();
    Map<String, Object> whereParams = new HashMap<>();
    private SearchParam params = null;
    private String groupBy = "";

    public enum LikePattern { Start, End, Both }

    private MySqlBuilder() {}

    public static MySqlBuilder build() {
        return new MySqlBuilder();
    }

    public static MySqlBuilder build(SearchParam params) {
        MySqlBuilder sqlBuilder = new MySqlBuilder();
        return sqlBuilder.whereParams(params);
    }

    public MySqlBuilder count(String count) {
        this.count.append(count);
        return this;
    }

    public MySqlBuilder select(String ...columns) {
        select.append(String.join(",", columns));
        return this;
    }

    public MySqlBuilder from(String table) {
        from.append(' ').append(table).append(' ');
        return this;
    }

    public MySqlBuilder innerJoin(String table, String on) {
        from.append(" INNER JOIN ").append(table).append(" ON ").append(on);
        return this;
    }
    public MySqlBuilder leftJoin(String table, String on) {
        from.append(" LEFT JOIN ").append(table).append(" ON ").append(on);
        return this;
    }

    public MySqlBuilder whereParams(SearchParam params) {
        this.params = params;
        return this;
    }

    public MySqlBuilder where(String column, String key) {
        return where(column, key, params.opt(key));
    }

    public MySqlBuilder where(String column, String key, Function<String, String> param) {
        return where(column, key, param.apply(key));
    }

    public <T> MySqlBuilder where(String column, String key, Function<String, String> param, Function<String, T> castFunction) {
        String value = param.apply(key);
        return where(column, key, value == null? null: castFunction.apply(value));
    }

    public <T> MySqlBuilder where(String column, String key, T value) {
        if (value == null || value instanceof String && StringUtils.isEmpty((String) value)) return this;
        if (value instanceof List) {
            List<Object> values = (List) value;
            if (values.isEmpty()) return this;
            if(values.size() == 1 ) {
                where.append(" AND ").append(column).append(" = :").append(key);
                whereParams.put(key, values.getFirst());
                return this;
            }
            where.append(" AND ").append(column).append(" IN (");
            for (int i = 0; i < values.size(); i++) {
                if (i != 0) where.append(", ");
                where.append(':').append(key).append(i);
                whereParams.put(key + i, values.get(i));
            }
            where.append(") ");
        } else {
            where.append(" AND ").append(column).append(" = :").append(key);
            whereParams.put(key, value);
        }
        return this;
    }

    public MySqlBuilder whereLike(String column, String key, LikePattern likePattern) {
        return whereLike(column, key, params.opt(key), likePattern);
    }

    public MySqlBuilder whereLike(String column, String key, LikePattern likePattern, Function<String, String> param) {
        return whereLike(column, key, param.apply(key), likePattern);
    }

    public MySqlBuilder whereLike(String column, String key, String value, LikePattern likePattern) {
        if (StringUtils.isEmpty(value)) return this;
        where.append(" AND ").append(column).append(" LIKE :").append(key);
        if (likePattern == LikePattern.Start || likePattern == LikePattern.Both) value = "%" + value;
        if (likePattern == LikePattern.End || likePattern == LikePattern.Both) value += "%";
        whereParams.put(key, value);
        return this;
    }

    public MySqlBuilder whereBetween(String column, String fromKey, String toKey) {
        String fromValue = params.opt(fromKey);
        String toValue = params.opt(toKey);
        return whereBetween(column, fromKey, toKey, fromValue, toValue);
    }

    public MySqlBuilder whereBetween(String column, String fromKey, String toKey, Function<String, String> param) {
        String fromValue = param.apply(fromKey);
        String toValue = param.apply(toKey);
        return whereBetween(column, fromKey, toKey, fromValue, toValue);
    }

    public MySqlBuilder whereBetween(String column, String fromKey, String toKey, String fromValue, String toValue) {
        if (fromValue == null || toValue == null) return this;
        LocalDate fromDate = LocalDate.parse(fromValue, dateFormat);
        LocalDate toDate = LocalDate.parse(toValue, dateFormat);
        if (fromDate.isAfter(toDate)) return this;
        return whereBetween(column, fromKey, toKey, fromDate, toDate);
    }

    public MySqlBuilder whereBetween(String column, String fromKey, String toKey, LocalDate fromDate, LocalDate toDate) {
        if (fromDate == null || toDate == null || fromDate.isAfter(toDate)) return this;
        where.append(" AND ").append(column).append(" between :").append(fromKey).append(" AND :").append(toKey);
        whereParams.put(fromKey, fromDate.toString());
        whereParams.put(toKey, toDate.toString());
        return this;
    }

    public MySqlBuilder groupBy(String groupBy) {
        this.groupBy = " GROUP BY " + groupBy;
        return this;
    }

    private String getSortByWithOrder(Map<String, String> sortMap, String sortBy) {
        String order = " ASC ";
        if (StringUtils.hasText(sortBy) && sortBy.startsWith("-")) {
            sortBy = sortBy.substring(1);
            order = " DESC ";
        }
        if (!StringUtils.hasText(sortBy) || !sortMap.containsKey(sortBy)) {
            if (sortMap.containsKey("default")) sortBy = "default";
            if (sortMap.containsKey("-default")) {
                sortBy = "default";
                order = " DESC ";
            }
        }
        if (!StringUtils.hasText(sortBy) || !sortMap.containsKey(sortBy)) return null;
        return sortMap.get(sortBy) + order;
    }

    public MySqlBuilder orderBy(Map<String, String> sortMap, String alwaysSortBy, String sortBy) {
        String sortByWithOrder = getSortByWithOrder(sortMap, alwaysSortBy);
        if (sortByWithOrder == null) return this;
        orderBy.append(" ORDER BY ").append(sortByWithOrder);
        String sortByWithOrder2 = getSortByWithOrder(sortMap, sortBy);
        if (sortByWithOrder.equals(sortByWithOrder2)) return this;
        if (sortByWithOrder2 != null) {
            orderBy.append(", ").append(sortByWithOrder2);
        }
        return this;
    }

    public MySqlBuilder orderBy(Map<String, String> sortMap, String sortBy) {
        String sortByWithOrder = getSortByWithOrder(sortMap, sortBy);
        if (sortByWithOrder != null) {
            orderBy.append(" ORDER BY ").append(sortByWithOrder);
        }
        return this;
    }

    public MySqlBuilder where(String s) {
        where.append(s);
        return this;
    }

    public String getCountQueryString() {
        return count.append(from).append(where).append(groupBy).append(orderBy).append(") x").toString();
    }

    public String getQueryString() {
        return select.append(from).append(where).append(groupBy).append(orderBy).toString();
    }

    public <T> Page<T> query(EntityManager em, Pageable pageRequest, Class<T> clazz) {
        String countQueryStr = getCountQueryString();
        Query countQuery = em.createNativeQuery(countQueryStr);
        whereParams.forEach(countQuery::setParameter);
        long total = ((Number)countQuery.getSingleResult()).longValue();
        if (total > 0) {
            String queryStr = getQueryString();
            Query query = em.createNativeQuery(queryStr, clazz);
            query.setMaxResults(pageRequest.getPageSize());
            query.setFirstResult((int)pageRequest.getOffset());
            whereParams.forEach(query::setParameter);
            @SuppressWarnings("unchecked") List<T> result = (List<T>)query.getResultList();
            return new PageImpl<>(result, pageRequest, total);
        }
        return new PageImpl<>(Collections.emptyList(), pageRequest, total);
    }

}
