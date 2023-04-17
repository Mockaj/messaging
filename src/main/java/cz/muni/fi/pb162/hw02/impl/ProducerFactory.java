package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import cz.muni.fi.pb162.hw02.mesaging.client.Producer;

import java.util.Collection;
//import java.util.Deque;
//import java.util.Iterator;
import java.util.LinkedList;
//import java.util.stream.Collectors;

/**
 * @author Ladislav Husty
 */
public class ProducerFactory implements Producer {
    private final Broker broker;

    /**
     * Constructor for ProducerFactory
     * @param broker
     */
    public ProducerFactory(Broker broker) {
        this.broker = broker;
    }

    @Override
    public Broker getBroker() {
        return new BrokerFactory();
    }

    @Override
    public Collection<String> listTopics() {
        return broker.listTopics();
    }

    @Override
    public Message produce(Message message) {
        LinkedList<Message> messages = new LinkedList<>();
        messages.push(message);
        Collection<Message> addedMessage = broker.push(messages);
        return addedMessage.iterator().next();
    }

    @Override
    public Collection<Message> produce(Collection<Message> messages) {
        LinkedList<Message> messagesList = new LinkedList<>(messages);
        return broker.push(messagesList);
    }
}
