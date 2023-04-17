package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import java.util.Map;
import java.util.Set;


/**
 * @author Ladislav Husty
 */
public class MessageFactory implements Message {

    private long id;
    private final Set<String> topics;
    private final Map<String, Object> data;

    /**
     *
     * @param message
     * @param id
     */
    public MessageFactory(Message message, long id){
        this.id = id;
        this.topics = message.topics();
        this.data = message.data();
    }

    /**
     * Constructor for Producer
     * @param topics
     * @param data
     */
    public MessageFactory(Set<String> topics, Map<String, Object> data) {
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

    public void setId(long id){
        this.id = id;
    }
}
