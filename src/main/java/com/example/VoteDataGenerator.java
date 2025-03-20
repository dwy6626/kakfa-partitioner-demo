package com.example;

import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.io.InputStream;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class VoteDataGenerator {

    final private Random random;
    private String techLead;
    private String[] candidates;
    private int techLeadRate;

    private int voteValueRangeLower = 1;
    private int voteValueRangeUpper = 1;

    @SuppressWarnings("unchecked")
    private void loadConfig() {
        Yaml yaml = new Yaml();
        try (InputStream inputStream = new FileInputStream("configuration/data-config.yaml")) {
            Map<String, Object> config = yaml.load(inputStream);
            techLead = (String) config.get("techLead");
            List<String> candidateList = (List<String>) config.get("candidates");
            techLeadRate = (Integer) config.get("techLeadRate");
            voteValueRangeLower = (Integer) config.get("voteValueRangeLower");
            voteValueRangeUpper = (Integer) config.get("voteValueRangeUpper");

            if (techLead == null || candidateList == null || techLeadRate == 0) {
                throw new RuntimeException("Missing required config values");
            }
            candidates = candidateList.toArray(new String[0]);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Config file not found. Please provide configuration/data-config.yaml", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration/data-config.yaml. Please check the file format and required fields.", e);
        }
    }

    public VoteDataGenerator() {
        this.random = new Random();
        loadConfig();
    }

    public ProducerRecord<String, String> generateVote(String topic) {
        String key;
        int r = random.nextInt(100);
        if (r < techLeadRate) {
            key = techLead;
        } else {
            key = candidates[random.nextInt(candidates.length)];
        }
        String value = String.valueOf(random.nextInt(voteValueRangeUpper - voteValueRangeLower) + voteValueRangeLower);
        return new ProducerRecord<>(topic, key, value);
    }

    public List<ProducerRecord<String, String>> generateVotes(String topic, int count) {
        List<ProducerRecord<String, String>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(generateVote(topic));
        }
        return records;
    }
}
