package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Ladislav Husty
 */
public class BrokerFactory implements Broker {
    private final Map<String, List<Message>> database;
    private static long messagesCounter = 0;

    /**
     * Constructor for broker
     */
    public BrokerFactory() {
        this.database = new HashMap<>();
    }

    @Override
    public Collection<String> listTopics() {
        return Collections.unmodifiableSet(database.keySet());
    }

    @Override
    public Collection<Message> push(Collection<Message> messages) {
        LinkedList<Message> populatedMessages = new LinkedList<>();
        for (Message message : messages) {
            messagesCounter++;
            Message newMessage = new MessageGeneral(message, messagesCounter); // create a new message with a new ID
            populatedMessages.push(newMessage);
            Set<String> topics = message.topics();
            for (String topic : topics) {
                List<Message> messagesList;
                if (database.containsKey(topic)) {
                    messagesList = database.get(topic);
                } else {
                    messagesList = new LinkedList<>();
                }
                messagesList.add(newMessage); // add the new message object to the messagesList
                messagesList.sort(Comparator.comparingLong(Message::id)); // sort messages by ID
                database.put(topic, messagesList);
            }
        }
        return populatedMessages;
    }

    @Override
    public Collection<Message> poll(Map<String, Long> offsets, int num, Collection<String> topics) {
        Set<Message> result = new LinkedHashSet<>();
        for (String topic : topics) {
            long lastReadId;
            if (offsets.get(topic) == null) {
                lastReadId = 0;
            } else {
                lastReadId = offsets.get(topic);
            }
            LinkedList<Message> messagesLinkedList = (LinkedList<Message>) database.get(topic);
            if (messagesLinkedList != null) {
                // Is iterator the preferred way to loop through a collection?
                // Should I be using it everywhere instead of forEach loop?
                Iterator<Message> messageIterator = messagesLinkedList.iterator();
                int count = 0;
                while (messageIterator.hasNext() && count < num) {
                    Message message = messageIterator.next();

                    if (message.id() == null || message.id() <= lastReadId) {
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
}
