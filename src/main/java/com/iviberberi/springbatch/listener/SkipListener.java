package com.iviberberi.springbatch.listener;

import com.iviberberi.springbatch.model.StudentCSV;
import com.iviberberi.springbatch.model.StudentJson;
import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.core.annotation.OnSkipInWrite;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.util.Date;

@Component
public class SkipListener {

    @OnSkipInRead
    public void skipInRead(Throwable th) {
        if (th instanceof FlatFileParseException) {
            createFile("C:\\Users\\ivi.berberi\\IdeaProjects\\spring-batch\\Chunk Job\\First Chunk Step\\Reader\\SkipInRead.txt",
                    ((FlatFileParseException) th).getInput());
        }
    }

    @OnSkipInProcess
    public void skipInProcess(StudentCSV studentCSV, Throwable th) {
        if (th instanceof NullPointerException) {
            createFile("C:\\Users\\ivi.berberi\\IdeaProjects\\spring-batch\\Chunk Job\\First Chunk Step\\Processor\\SkipInProcess.txt",
                    studentCSV.toString());
        }
    }

    @OnSkipInWrite
    public void skipInWriter(StudentJson studentJson, Throwable th) {
        if (th instanceof NullPointerException) {
            createFile("C:\\Users\\ivi.berberi\\IdeaProjects\\spring-batch\\Chunk Job\\First Chunk Step\\Writer\\SkipInWriter.txt",
                    studentJson.toString());
        }
    }

    public void createFile(String filePath, String data) {
        try (FileWriter fileWriter = new FileWriter(new File(filePath), true)) {
            fileWriter.write(data + "," + new Date() + "\n");
        } catch (Exception e) {

        }
    }
}
