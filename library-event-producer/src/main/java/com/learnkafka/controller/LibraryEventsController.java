package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Slf4j
@RestController
public class LibraryEventsController {

  @Autowired
  LibraryEventProducer libraryEventProducer;

  @PostMapping("/v1/libraryevent")
  public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
//        libraryEventProducer.sendLibraryEvent(libraryEvent);
//    SendResult<Integer, String> stringSendResult = libraryEventProducer.synchronousSendLibraryEvent(libraryEvent);
//    log.info("Send result: {}", stringSendResult.toString());
    libraryEvent.setLibraryEventType(LibraryEventType.NEW);
    libraryEventProducer.sendLibraryEventMethod2(libraryEvent);
    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }

  @PutMapping("/v1/libraryevent")
  public ResponseEntity<?> putLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
//        libraryEventProducer.sendLibraryEvent(libraryEvent);
//    SendResult<Integer, String> stringSendResult = libraryEventProducer.synchronousSendLibraryEvent(libraryEvent);
//    log.info("Send result: {}", stringSendResult.toString());
    if (libraryEvent.getLibraryEventId() == null) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please provide libraryEventId");
    }
    libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
    libraryEventProducer.sendLibraryEventMethod2(libraryEvent);
    return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
  }
}
