package com.epam.course.kafka.streams.task4;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Message {

    private String name;
    private String company;
    private String position;
    private int experience;
}
