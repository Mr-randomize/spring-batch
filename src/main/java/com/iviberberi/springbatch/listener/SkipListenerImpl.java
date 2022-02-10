package com.iviberberi.springbatch.listener;

import com.iviberberi.springbatch.model.StudentCSV;
import com.iviberberi.springbatch.model.StudentJson;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.util.Date;

@Component
public class SkipListenerImpl implements SkipListener<StudentCSV, StudentJson> {
    @Override
    public void onSkipInRead(Throwable throwable) {
        if (throwable instanceof FlatFileParseException) {
            createFile("C:\\Users\\ivi.berberi\\IdeaProjects\\spring-batch\\Chunk Job\\First Chunk Step\\Reader\\SkipInRead.txt",
                    ((FlatFileParseException) throwable).getInput());
        }
    }

    @Override
    public void onSkipInWrite(StudentJson studentJson, Throwable throwable) {
        if (throwable instanceof NullPointerException) {
            createFile("C:\\Users\\ivi.berberi\\IdeaProjects\\spring-batch\\Chunk Job\\First Chunk Step\\Writer\\SkipInWriter.txt",
                    studentJson.toString());
        }
    }

    @Override
    public void onSkipInProcess(StudentCSV studentCSV, Throwable throwable) {
        if (throwable instanceof NullPointerException) {
            createFile("C:\\Users\\ivi.berberi\\IdeaProjects\\spring-batch\\Chunk Job\\First Chunk Step\\Processor\\SkipInProcess.txt",
                    studentCSV.toString());
        }
    }

    public void createFile(String filePath, String data) {
        try (FileWriter fileWriter = new FileWriter(new File(filePath), true)) {
            fileWriter.write(data + "," + new Date() + "\n");
        } catch (Exception e) {

        }
    }
}
