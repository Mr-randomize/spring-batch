package com.iviberberi.springbatch.config;

import com.iviberberi.springbatch.listener.SkipListener;
import com.iviberberi.springbatch.listener.SkipListenerImpl;
import com.iviberberi.springbatch.model.*;
import com.iviberberi.springbatch.processor.FirstItemProcessor;
import com.iviberberi.springbatch.processor.StudentProcessor;
import com.iviberberi.springbatch.reader.FirstItemReader;
import com.iviberberi.springbatch.service.StudentService;
import com.iviberberi.springbatch.writer.CSVItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.adapter.ItemWriterAdapter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

@Configuration
public class SampleJobV2 {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;


    @Autowired
    private FirstItemReader firstItemReader;
    @Autowired
    private FirstItemProcessor firstItemProcessor;
    @Autowired
    private StudentProcessor studentProcessor;
    @Autowired
    private CSVItemWriter csvItemWriter;

    @Autowired
    private DatasourceConfig datasourceConfig;

    @Autowired
    private StudentService studentService;

    @Autowired
    private SkipListener skipListener;

    @Autowired
    private SkipListenerImpl skipListenerImpl;

    @Bean
    public Job chunkJob() {
        return jobBuilderFactory.get("Chunk Job")
                .incrementer(new RunIdIncrementer())
                .start(firstChunkStep())
                .build();
    }

    private Step firstChunkStep() {
        return stepBuilderFactory.get("First Chunk Step")
                .<StudentCSV, StudentJson>chunk(3)
//                .reader(jdbcCursorItemReader())
//                .reader(itemReaderAdapter())
//                .reader(staxEventItemReader(null))
//                .reader(jsonJsonItemReader(null))
                .reader(flatFileItemReader(null))
                .processor(studentProcessor)
//                .writer(csvItemWriter)
//                .writer(flatFileItemWriter(null))
                .writer(jsonFileItemWriter(null))
//                .writer(staxEventItemWriter(null))
//                .writer(jdbcBatchItemWriter1())
//                .writer(itemWriterAdapter())
                .faultTolerant()
                .skip(Throwable.class)
//                .skip(FlatFileParseException.class)
//                .skip(NullPointerException.class)
//                .skipLimit(Integer.MAX_VALUE)
                //skipPolicy && retryLimit cause loop when together
                .skipLimit(100)
//                .skipPolicy(new AlwaysSkipItemSkipPolicy())
                .retryLimit(3)
                .retry(Throwable.class)
//                .listener(skipListener)
                .listener(skipListenerImpl)
                .build();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<StudentCSV> flatFileItemReader(@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {
        FlatFileItemReader<StudentCSV> flatFileItemReader = new FlatFileItemReader<StudentCSV>();

        flatFileItemReader.setResource(fileSystemResource);

//        flatFileItemReader.setResource(new FileSystemResource(
//                new File("C:\\Users\\ivi.berberi\\IdeaProjects\\spring-batch\\InputFiles\\students.csv")));

        flatFileItemReader.setLineMapper(new DefaultLineMapper<StudentCSV>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames("ID", "First Name", "Last Name", "Email");
//                        setDelimiter("|");
                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<StudentCSV>() {
                    {
                        setTargetType(StudentCSV.class);
                    }
                });
            }
        });

        /*
		DefaultLineMapper<StudentCsv> defaultLineMapper =
				new DefaultLineMapper<StudentCsv>();

		DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
		delimitedLineTokenizer.setNames("ID", "First Name", "Last Name", "Email");

		defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);

		BeanWrapperFieldSetMapper<StudentCsv> fieldSetMapper =
				new BeanWrapperFieldSetMapper<StudentCsv>();
		fieldSetMapper.setTargetType(StudentCsv.class);

		defaultLineMapper.setFieldSetMapper(fieldSetMapper);

		flatFileItemReader.setLineMapper(defaultLineMapper);
		*/

        flatFileItemReader.setLinesToSkip(1);

        return flatFileItemReader;
    }

    // JSON ItemReader
    @Bean
    @StepScope
    public JsonItemReader<StudentJson> jsonJsonItemReader(@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {
        JsonItemReader<StudentJson> jsonJsonItemReader = new JsonItemReader<>();

        jsonJsonItemReader.setResource(fileSystemResource);
        jsonJsonItemReader.setJsonObjectReader(new JacksonJsonObjectReader<>(StudentJson.class));

        jsonJsonItemReader.setMaxItemCount(8);
        jsonJsonItemReader.setCurrentItemCount(2);

        return jsonJsonItemReader;
    }

    //Xml Item Reader
    @Bean
    @StepScope
    public StaxEventItemReader<StudentXml> staxEventItemReader(@Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {
        StaxEventItemReader<StudentXml> staxEventItemReader = new StaxEventItemReader<>();

        staxEventItemReader.setResource(fileSystemResource);
        staxEventItemReader.setFragmentRootElementName("student");
        staxEventItemReader.setUnmarshaller(new Jaxb2Marshaller() {
            {
                setClassesToBeBound(StudentXml.class);
            }
        });

        return staxEventItemReader;
    }

    //Jdbc Item Reader
    @Bean
    @StepScope
    public JdbcCursorItemReader<StudentJdbc> jdbcCursorItemReader() {
        JdbcCursorItemReader<StudentJdbc> jdbcCursorItemReader = new JdbcCursorItemReader<>();

        jdbcCursorItemReader.setDataSource(datasourceConfig.universityDataSource());
        jdbcCursorItemReader.setSql("SELECT id,first_name as FirstName, last_name as lastName, email from student");
        jdbcCursorItemReader.setRowMapper(new BeanPropertyRowMapper<>() {
            {
                setMappedClass(StudentJdbc.class);
            }
        });

//        jdbcCursorItemReader.setCurrentItemCount(2);
//        jdbcCursorItemReader.setMaxItemCount(8);

        return jdbcCursorItemReader;
    }

    //REST Item Reader
    @Bean
    @StepScope
    public ItemReaderAdapter<StudentResponse> itemReaderAdapter() {
        ItemReaderAdapter<StudentResponse> itemReaderAdapter = new ItemReaderAdapter<>();

        itemReaderAdapter.setTargetObject(studentService);
        itemReaderAdapter.setTargetMethod("getStudent");
        itemReaderAdapter.setArguments(new Object[]{1L, "Test"});

        return itemReaderAdapter;
    }

    //CSV Item Writer
    @Bean
    @StepScope
    public FlatFileItemWriter<StudentJdbc> flatFileItemWriter(@Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource) {
        FlatFileItemWriter<StudentJdbc> flatFileItemWriter = new FlatFileItemWriter<>();

        flatFileItemWriter.setResource(fileSystemResource);
        flatFileItemWriter.setHeaderCallback(writer -> writer.write("Id,First Name,Last Name,Email"));
        flatFileItemWriter.setLineAggregator(new DelimitedLineAggregator<>() {
            {
//                setDelimiter("|");
                setFieldExtractor(new BeanWrapperFieldExtractor<>() {
                    {
                        setNames(new String[]{"id", "firstName", "lastName", "email"});
                    }
                });
            }
        });

        flatFileItemWriter.setFooterCallback(writer -> writer.write("Created @" + new Date()));

        return flatFileItemWriter;
    }

    //JSON Item Writer
    @StepScope
    @Bean
    public JsonFileItemWriter<StudentJson> jsonFileItemWriter(
            @Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource) {
        JsonFileItemWriter<StudentJson> jsonFileItemWriter =
                new JsonFileItemWriter<>(fileSystemResource,
                        new JacksonJsonObjectMarshaller<>()) {
                    @Override
                    public String doWrite(List<? extends StudentJson> items) {
                        items.stream().forEach(item -> {
                            if (item.getId() == 3) {
                                throw new NullPointerException();
                            }
                        });
                        return super.doWrite(items);
                    }
                };

        return jsonFileItemWriter;
    }

    //Xml Item Writer
    @StepScope
    @Bean
    public StaxEventItemWriter<StudentJdbc> staxEventItemWriter(
            @Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource) {
        StaxEventItemWriter<StudentJdbc> staxEventItemWriter =
                new StaxEventItemWriter<StudentJdbc>();

        staxEventItemWriter.setResource(fileSystemResource);
        staxEventItemWriter.setRootTagName("students");

        staxEventItemWriter.setMarshaller(new Jaxb2Marshaller() {
            {
                setClassesToBeBound(StudentJdbc.class);
            }
        });

        return staxEventItemWriter;
    }

    @Bean
    public JdbcBatchItemWriter<StudentCSV> jdbcBatchItemWriter() {
        JdbcBatchItemWriter<StudentCSV> jdbcBatchItemWriter =
                new JdbcBatchItemWriter<StudentCSV>();

        jdbcBatchItemWriter.setDataSource(datasourceConfig.universityDataSource());
        jdbcBatchItemWriter.setSql(
                "insert into student(id, first_name, last_name, email) "
                        + "values (:id, :firstName, :lastName, :email)");

        jdbcBatchItemWriter.setItemSqlParameterSourceProvider(
                new BeanPropertyItemSqlParameterSourceProvider<StudentCSV>());

        return jdbcBatchItemWriter;
    }

    @Bean
    public JdbcBatchItemWriter<StudentCSV> jdbcBatchItemWriter1() {
        JdbcBatchItemWriter<StudentCSV> jdbcBatchItemWriter =
                new JdbcBatchItemWriter<StudentCSV>();

        jdbcBatchItemWriter.setDataSource(datasourceConfig.universityDataSource());
        jdbcBatchItemWriter.setSql(
                "insert into student(id, first_name, last_name, email) "
                        + "values (?,?,?,?)");

        jdbcBatchItemWriter.setItemPreparedStatementSetter(
                (item, ps) -> {
                    ps.setLong(1, item.getId());
                    ps.setString(2, item.getFirstName());
                    ps.setString(3, item.getLastName());
                    ps.setString(4, item.getEmail());
                });

        return jdbcBatchItemWriter;
    }

    public ItemWriterAdapter<StudentCSV> itemWriterAdapter() {
        ItemWriterAdapter<StudentCSV> itemWriterAdapter =
                new ItemWriterAdapter<StudentCSV>();

        itemWriterAdapter.setTargetObject(studentService);
        itemWriterAdapter.setTargetMethod("restCallToCreateStudent");

        return itemWriterAdapter;
    }
}
