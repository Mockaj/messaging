package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import cz.muni.fi.pb162.hw02.mesaging.client.Producer;

import java.util.Collection;
import java.util.LinkedList;

/**
 * @param broker
 * @author Ladislav Husty
 */
public record ProducerFactory(Broker broker) implements Producer {
    @Override
    public Broker broker() {
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
