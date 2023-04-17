package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import cz.muni.fi.pb162.hw02.mesaging.client.Consumer;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
//import java.util.stream.Collectors;

/**
 * @author Ladislav Husty
 */
public class ConsumerFactory implements Consumer {
    private final Broker broker;
    private Map<String, Long> offsets;

    /**
     * Constructor for ConsumerFactory
     * @param broker
     */
    public ConsumerFactory(Broker broker) {
        this.broker = broker;
        this.offsets = new HashMap<>();
    }

    @Override
    public Broker getBroker() {
        return new BrokerFactory();
    }

    @Override
    public Collection<String> listTopics() {
        return this.broker.listTopics();
    }

    @Override
    public Collection<Message> consume(int num, String... topics) {
        Map<String, Long> initialOffsets = Map.copyOf(getOffsets());
        for (String topic: topics){
            Collection<Message> messagesSingleTopic = broker.poll(offsets,
                    num,
                    Collections.singleton(topic));
            LinkedList<Message> messagesLinkedList = new LinkedList<>(messagesSingleTopic);
            messagesLinkedList.sort(Comparator.comparingLong(Message::id));
            LinkedList<Message> filteredMessages = messagesLinkedList.stream()
                    .filter(m -> m.topics().contains(topic))
                    .collect(Collectors.toCollection(LinkedList::new));
            if (filteredMessages.isEmpty()){
                continue;
            }
            try {
                this.offsets.put(topic, filteredMessages.get(num-1).id());
            } catch (IndexOutOfBoundsException e){
                    this.offsets.put(topic, filteredMessages.getLast().id());
                }
            }
        return broker.poll(initialOffsets, num, List.of(topics));
    }

    @Override
    public Collection<Message> consume(Map<String, Long> offsets, int num, String... topics) {
        Set<Message> returnedMessages = new HashSet<>();
        for (String topic : topics) {
            Collection<Message> polledMessages = broker.poll(
                    offsets,
                    num,
                    Collections.singleton(topic)
            );
            LinkedList<Message> linkedListMessages = new LinkedList<>(polledMessages);
            returnedMessages.addAll(linkedListMessages);
        }
        return returnedMessages;
    }

    @Override
    public Map<String, Long> getOffsets() {
        return new HashMap<>(offsets);
    }

    @Override
    public void setOffsets(Map<String, Long> offsets) {
        this.offsets.clear();
        this.offsets.putAll(offsets);
    }

//    private  Map<String, Long> getInitialOffsets(){
//        Map<String, Long> initialOffsets = new HashMap<>();
//        for (String topic: listTopics()){
//            LinkedList<Message> firstUnreadMessage = (LinkedList<Message>) broker.poll(
//                    Map.of(),
//                    1,
//                    Collections.singleton(topic));
//            initialOffsets.put(topic, firstUnreadMessage.getFirst().id());
//        }
//        return initialOffsets;
//    }

    @Override
    public void clearOffsets() {
        this.offsets.clear();
    }

    @Override
    public void updateOffsets(Map<String, Long> offsets) {
        for (Map.Entry<String, Long> entry : offsets.entrySet()) {
            if (this.offsets.containsKey(entry.getKey())) {
                this.offsets.put(entry.getKey(), entry.getValue());
            }
        }
    }

}

