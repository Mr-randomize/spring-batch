package com.iviberberi.springbatch.writer;

import com.iviberberi.springbatch.model.*;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CSVItemWriter implements ItemWriter<StudentJdbc> {

    @Override
    public void write(List<? extends StudentJdbc> list) throws Exception {
        System.out.println("Inside item writer");
        list.stream().forEach(System.out::println);
    }
}
