package com.epam.bigdata2016.minskq3.flume;


import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.mina.core.RuntimeIoException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TagsInterseptor implements Interceptor {

    private String vokPath;
    private String timeHeader;
    private String hasHeaderTag;
    private Map<String, String> vocabulary;

    public TagsInterseptor(String vokPath, String timeHeader, String hasHeaderTag) {
        this.vokPath = vokPath;
        this.timeHeader = timeHeader;
        this.hasHeaderTag = hasHeaderTag;
    }

    @Override
    public void initialize() {
        try (Stream<String> lines = Files.lines(Paths.get(vokPath))) {
            vocabulary = lines
                    .skip(1)
                    .map(line -> line.split("\\t"))
                    .collect(Collectors.toMap(
                            lineArr -> lineArr[0], //key -id
                            lineArr -> lineArr[1]  //value --tags string
                    ));
        } catch (IOException ex) {
            throw new RuntimeIoException(ex);
        }
    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        List<String> messageFields = getMessageFields(event.getBody());

        String userTags = getUserTags(messageFields.get(20));
        String eventDate = messageFields.get(1).substring(0, 8);

        headers.put(timeHeader, eventDate);
        headers.put(hasHeaderTag, StringUtils.isNotBlank(userTags) ? "true" : "false" );
        messageFields.add(userTags);

        event.setHeaders(headers);
        event.setBody(String.join("\t", messageFields).getBytes());

        return event;
    }

    private List<String> getMessageFields(byte[] messageBody){

        String text = new String(messageBody);
        return new ArrayList<>(Arrays.asList(text.split("\\t")));
    }

    private String getUserTags(String userTagsId) {

        return vocabulary.getOrDefault(userTagsId, "");
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        return list.stream()
                .map(this::intercept)
                .collect(Collectors.toList());
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        private String vokPath;
        private String timeHeader;
        private String hasHeaderTag;

        @Override
        public Interceptor build() {
            return new TagsInterseptor(vokPath, timeHeader, hasHeaderTag);
        }

        @Override
        public void configure(Context context) {
            vokPath = context.getString("vokPath");
            timeHeader = context.getString("timeHeader");
            hasHeaderTag = context.getString("hasHeaderTag");
        }
    }
}
