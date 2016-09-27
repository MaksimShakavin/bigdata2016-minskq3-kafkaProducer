package com.epam.bigdata2016.minskq3.flume;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.mina.core.RuntimeIoException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TagsInterseptor implements Interceptor {

    private String vokPath;
    private String tagsHeader;
    private String hasHeaderTag;
    private Map<String, String> vocabulary;

    public TagsInterseptor(String vokPath, String tagsHeader, String hasHeaderTag) {
        this.vokPath = vokPath;
        this.tagsHeader = tagsHeader;
        this.hasHeaderTag = hasHeaderTag;
    }

    @Override
    public void initialize() {
        try (Stream<String> lines = Files.lines(Paths.get(vokPath))) {
            lines
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
        Optional.of(event)
                .map(e -> new String(e.getBody()).split("\\t"))
                .map(arr -> arr[arr.length - 2])
                .map(vocabulary::get)
                .ifPresent(tags -> {
                    Map<String, String> headers = event.getHeaders();
                    headers.put(hasHeaderTag, "true");
                    event.getHeaders().put(tagsHeader, tags);
                });
        return event;
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

    public static class Builderrr implements Interceptor.Builder {
        private String vokPath;
        private String tagsHeader;
        private String hasHeaderTag;

        @Override
        public Interceptor build() {
            return new TagsInterseptor(vokPath, tagsHeader, hasHeaderTag);
        }

        @Override
        public void configure(Context context) {
            vokPath = context.getString("vokPath");
            tagsHeader = context.getString("tagsHeader");
            hasHeaderTag = context.getString("hasHeaderTag");
        }
    }
}
