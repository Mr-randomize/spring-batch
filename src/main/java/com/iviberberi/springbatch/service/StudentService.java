package com.iviberberi.springbatch.service;

import com.iviberberi.springbatch.model.StudentCSV;
import com.iviberberi.springbatch.model.StudentResponse;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class StudentService {
    List<StudentResponse> list;


    public List<StudentResponse> restCallToGetStudents() {
        RestTemplate restTemplate = new RestTemplate();
        StudentResponse[] studentResponseArray = restTemplate.getForObject("http://localhost:8081", StudentResponse[].class);

        list = new ArrayList<>();

        Arrays.stream(studentResponseArray).forEach(studentResponse -> list.add(studentResponse));

        return list;
    }

    public StudentResponse getStudent(long id, String name) {
        System.out.println("id = " + id + " and name = " + name);

        if (list == null) {
            restCallToGetStudents();
        } else if (list != null && !list.isEmpty()) {
            return list.remove(0);
        }
        return null;
    }

    public StudentResponse restCallToCreateStudent(StudentCSV studentCSV) {
        RestTemplate restTemplate = new RestTemplate();
        return restTemplate.postForObject("http://localhost:8081/api/v1/createStudent", studentCSV, StudentResponse.class);
    }
}
