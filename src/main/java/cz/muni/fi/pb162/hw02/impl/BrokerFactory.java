package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * @author Ladislav Husty
 */
public class BrokerFactory implements Broker {

    private final Map<String, Stack<Message>> database;
    private static long messagesCounter = 0;

    /**
     * Constructor for broker
     */
    public BrokerFactory(){
        this.database = new HashMap<>();
    }
    @Override
    public Collection<String> listTopics() {
        return database.keySet();
    }

    @Override
    public Collection<Message> push(Collection<Message> messages) {
        Stack<Message> populatedMessages = new Stack<>();
        for (Message messageProducer : messages) {
            messagesCounter = messagesCounter + 1;
            Message messageBroker = new MessageFactory(messageProducer.topics(),
                    messageProducer.data(),
                    messagesCounter);
            populatedMessages.push(messageBroker);
            Set<String> topics = messageBroker.topics();
            for (String topic: topics) {
                Stack<Message> messagesStack;
                if (database.containsKey(topic)) {
                    messagesStack = database.get(topic);
                } else{
                    messagesStack = new Stack<>();
                }
                messagesStack.push(messageBroker);
                database.put(topic, messagesStack);
            }
        }
        return populatedMessages;
    }

    @Override
    public Collection<Message> poll(Map<String, Long> offsets, int num, Collection<String> topics) {
        Set<Message> result = new HashSet<>();

        for (String topic : topics) {
            long lastReadId = offsets.getOrDefault(topic, 0L); // default to 0 if not found
            Stack<Message> messagesStack = database.get(topic);

            if (messagesStack != null) {
                Iterator<Message> messageIterator = messagesStack.iterator();
                int count = 0;

                while (messageIterator.hasNext() && count < num) {
                    Message message = messageIterator.next();

                    if (message.id() <= lastReadId) {
                        // Message has already been read, skip it
                        continue;
                    }
                    result.add(message);
                    count++;
                }
            }
        }
        return result;
    }

    /**
     *
     * @return map
     */
    // DONT FORGET TO DELETE THIS
    public Map<String, Stack<Message>> getDatabase() {
        return this.database;
    }

}
