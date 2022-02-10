package com.iviberberi.springbatch.controller;

import com.iviberberi.springbatch.request.JobParamsRequest;
import com.iviberberi.springbatch.service.JobService;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/job")
public class JobController {

    @Autowired
    JobService jobService;

    @Autowired
    JobOperator jobOperator;

    @GetMapping("/start/{jobName}")
    public String startJob(@PathVariable String jobName, @RequestBody List<JobParamsRequest> jobParamsRequestList) throws Exception {

        jobService.startJob(jobName, jobParamsRequestList);
        return "Job started ...";
    }

    @GetMapping("/stop/{jobExecutionId}")
    public String stopJob(@PathVariable long jobExecutionId) {
        try {
            jobOperator.stop(jobExecutionId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Job Stopped...";
    }
}
