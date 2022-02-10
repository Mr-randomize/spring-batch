package com.iviberberi.springbatch.processor;

import com.iviberberi.springbatch.model.StudentCSV;
import com.iviberberi.springbatch.model.StudentJdbc;
import com.iviberberi.springbatch.model.StudentJson;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class StudentProcessor implements ItemProcessor<StudentCSV, StudentJson> {
    @Override
    public StudentJson process(StudentCSV item) throws Exception {
        System.out.println("Inside Item Processor");

        if(item.getId() == 6){
            throw new NullPointerException();
        }

        StudentJson studentJson = new StudentJson();
        studentJson.setId(item.getId());
        studentJson.setFirstName(item.getFirstName());
        studentJson.setLastName(item.getLastName());
        studentJson.setEmail(item.getEmail());
        return studentJson;
    }
}
