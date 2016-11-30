package com.bytro.firefly.stream;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yunus on 30.11.16.
 */
public class SingleStringMapper implements Function<String, String>{
    private final String outputFormat;
    private final Function<String, String> mapper;
    private final Pattern inputPattern;

    public SingleStringMapper(String inputRegex, String outputFormat, Function<String, String> mapper) {
        this.inputPattern = Pattern.compile(inputRegex);
        this.outputFormat = outputFormat;
        this.mapper = mapper;
    }

    @Override
    public String apply(String input) {
        Matcher matcher = inputPattern.matcher(input);
        if (matcher.matches()) {
            String replacedValue = mapper.apply(matcher.group(1));
            if (replacedValue != null)
                return String.format(outputFormat, replacedValue);
        }
        return "";
    }
}
