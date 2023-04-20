package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Message;

import java.util.Map;
import java.util.Set;

/**
 * @author Ladislav Husty
 */
public class MessageGeneral implements Message {

    private long id;
    private final Set<String> topics;
    private final Map<String, Object> data;

    /**
     * @param message
     * @param id
     */
    public MessageGeneral(Message message, long id) {
        this.id = id;
        this.topics = message.topics();
        this.data = message.data();
    }

    /**
     * Constructor for creating Message from scratch
     * not needed in this code, but I find it essential
     * if someone else wanted to "use this code"
     *
     * @param topics
     * @param data
     */
    public MessageGeneral(Set<String> topics, Map<String, Object> data) {
        this.id = -1;
        this.topics = topics;
        this.data = data;
    }

    @Override
    public Long id() {
        return this.id;
    }

    @Override
    public Set<String> topics() {
        return this.topics;
    }

    @Override
    public Map<String, Object> data() {
        return this.data;
    }

    public void setId(long id) {
        this.id = id;
    }
}
