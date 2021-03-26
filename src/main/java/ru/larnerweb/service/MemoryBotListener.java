package ru.larnerweb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Update;
import ru.larnerweb.config.BotConfig;

@Log4j2
@Service
@AllArgsConstructor
public class MemoryBotListener extends TelegramLongPollingBot {

    private final BotConfig config;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<Integer, String> kafkaTemplate;

    private final String SOURCE_TOPIC = "requests";
    private final String TARGET_TOPIC = "responses";

    @Override
    @SneakyThrows
    public void onUpdateReceived(Update update) {
        String updateJson = objectMapper.writeValueAsString(update);
        log.info("Received update: {}", updateJson);
        kafkaTemplate.send(SOURCE_TOPIC, update.getUpdateId(), updateJson);
    }

    @SneakyThrows
    @KafkaListener(topics = TARGET_TOPIC)
    public void listenGroupFoo(String message) {
        log.info("Received Message in group foo: " + message);
        Update update = objectMapper.readValue(message, Update.class);

        SendMessage sendMessage = new SendMessage();
        sendMessage.setChatId(update.getMessage().getChatId().toString());
        sendMessage.setText(update.getMessage().getText());

        execute(sendMessage);
    }

    @Override
    public String getBotToken() {
        return config.getBotToken();
    }

    @Override
    public String getBotUsername() {
        return config.getBotName();
    }
}
