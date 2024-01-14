package st.gin.dto;

import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import java.util.function.Function;

public record SearchParam(MultiValueMap<String, String> params) {

    public String opt(String name) {
        if (params.get(name) == null || StringUtils.isEmpty(params.get(name).getFirst())) {
            return null;
        }
        return params.get(name).getFirst();
    }

    public String req(String name) {
        if (params.get(name) == null || StringUtils.isEmpty(params.get(name).getFirst())) {
            throw new RuntimeException("Parameter " + name + " is mandatory");
        }
        return params.get(name).getFirst();
    }

    public <T> T opt(String name, Function<String, T> castFunction) {
        String value = opt(name);
        if (value == null) {
            return null;
        }
        return castFunction.apply(value);
    }

    public <T> T req(String name, Function<String, T> castFunction) {
        String value = req(name);
        if (value == null) {
            return null;
        }
        return castFunction.apply(value);
    }

}
