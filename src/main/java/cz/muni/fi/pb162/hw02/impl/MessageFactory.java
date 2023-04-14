package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import java.util.Map;
import java.util.Set;


/**
 * @author Ladislav Husty
 */
public class MessageFactory implements Message {

    private final long id;
    private final Set<String> topics;
    private Map<String, Object> data;

    /**
     * Constructor for Broker
     * @param topics
     * @param data
     * @param id
     */
    public MessageFactory(Set<String> topics, Map<String, Object> data, long id){
        this.id = id;
        this.topics = topics;
        this.data = data;
    }

    /**
     * Constructor for Producer
     * @param topics
     * @param data
     */
    public MessageFactory(Set<String> topics, Map<String, Object> data) {
        this(topics, data, -1);
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
}
