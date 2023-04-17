package cz.muni.fi.pb162.hw02.impl;

//import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
//
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Set;
//import java.util.Stack;

import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import cz.muni.fi.pb162.hw02.mesaging.client.Producer;

//import java.util.Collection;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.HashSet;
//import java.util.Iterator;
//import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
//import java.util.Stack;
//import java.util.stream.Collectors;

/**
 * @author Ladislav Husty
 */
public class Main {
    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        Set<String> topics1 = new HashSet<>(Collections.singleton("Cars"));
        Set<String> topics2 = new HashSet<>(Collections.singleton("Sport"));
        Set<String> topics3 = new HashSet<>(Collections.singleton("Entertainment"));
        Map<String, Object> data1 = new HashMap<>();
        data1.put("Volvo, Toyota", new Object());
        Map<String, Object> data2 = new HashMap<>();
        data2.put("Calisthenics, Tennis", new Object());
        Map<String, Object> data3 = new HashMap<>();
        data3.put("Gaming, Coding", new Object());

        Message message1 = new MessageFactory(topics1, data1);
        Message message2 = new MessageFactory(topics1, data2);
        Message message3 = new MessageFactory(topics3, data3);

        LinkedList<Message> messages = new LinkedList<>();
        messages.add(message1);
        messages.add(message2);
        messages.add(message3);

        BrokerFactory broker = new BrokerFactory();
        Producer producer = new ProducerFactory(broker);



        Map<String, LinkedList<Message>> databaseDmpty = broker.getDatabase();
//        System.out.println(databaseDmpty.toString());
        Collection<Message> pushedMessage = broker.push(messages);
        Map<String, LinkedList<Message>> databasePopulated = broker.getDatabase();
//        System.out.println(databasePopulated.toString());
        Map<String, LinkedList<Message>> database = broker.getDatabase();
        LinkedList<Message> messageCars = database.get("Cars");

        Map<String, Object> data = Map.of("name", "Tom", "age", 3);
        Message mess = new MessageFactory(Set.of("TOPIC_HOUSE", "TOPIC_GARDEN"), data);
        LinkedList<Message> messageLinkedList = new LinkedList<>();
        messageLinkedList.add(mess);
        var batch = broker.push(messageLinkedList);

        Map<String, LinkedList<Message>> database2 = broker.getDatabase();
        for (String topic: database2.keySet()) {
            for (Message msg: database2.get(topic)){
                System.out.println(topic + " = " + msg.id());
            }

        }
//        Set<String> topics1 = new HashSet<>(Collections.singleton("Cars"));

        Collection<Message> polled = broker.poll(Map.of(), 1, topics1);
        for (Message message : polled){
//            System.out.println(message.id() + " = " + message.data());
        }

        batch = producer.produce(List.of(
                new MessageFactory(Collections.singleton("TOPIC_HOUSE"), Map.of("name", "Jerry")),
                new MessageFactory(Collections.singleton("TOPIC_GARDEN"), Map.of("name", "Nibbles"))
        ));

        for (Message message : batch){
            System.out.println(message.id());
        }
    }
}
