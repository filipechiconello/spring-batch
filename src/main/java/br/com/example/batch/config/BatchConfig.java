package br.com.example.batch.config;

import br.com.example.batch.model.Products;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchConfig {

    @Autowired
    @Qualifier("transactionManagerApp")
    private PlatformTransactionManager transactionManager;

    @Bean
    public Job importProductJob(JobRepository jobRepository, Step step) {
        return new JobBuilder("importProductJob", jobRepository)
                .start(step)
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public Step importProductsStep(ItemReader<Products> reader, ItemWriter<Products> writer, JobRepository jobRepository) {
        return new StepBuilder("importProductsStep", jobRepository)
                .<Products, Products>chunk(200, transactionManager)
                .reader(reader)
                .writer(writer)
                .build();
    }

    @Bean
    public ItemReader<Products> reader() {
        return new FlatFileItemReaderBuilder<Products>()
                .name("reader")
                .resource(new FileSystemResource("files/products.csv"))
                .comments("--")
                .delimited()
                .names("id", "name", "price", "quantity")
                .targetType(Products.class)
                .build();
    }

    @Bean
    public ItemWriter<Products> writer(@Qualifier("appDS") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Products>()
                .dataSource(dataSource)
                .sql("INSERT INTO products (id, name, price, quantity) VALUES (:id, :name, :price, :quantity)")
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }
}