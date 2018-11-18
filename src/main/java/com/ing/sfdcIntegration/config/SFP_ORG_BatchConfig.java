package com.ing.sfdcIntegration.config;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import com.ing.sfdcIntegration.bean.PartyDataBean;
import com.ing.sfdcIntegration.processor.SFP_ORG_ItemProcessor;

@Configuration
@EnableBatchProcessing
public class SFP_ORG_BatchConfig {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Bean
	public Job readFlatFileJob() {
		return jobBuilderFactory.get("readFlatFileJob").incrementer(new RunIdIncrementer()).start(step()).build();
	}

	@Bean
	public Step step() {
		return stepBuilderFactory.get("step").<PartyDataBean, PartyDataBean>chunk(5).reader(sfpOrgItemreader())
				.processor(sfpOrgItemprocessor()).writer(compositeItemWriter()).build();
	}

	@Bean
	public ItemProcessor<PartyDataBean, PartyDataBean> sfpOrgItemprocessor() {
		return new SFP_ORG_ItemProcessor();
	}

	@Bean
	public FlatFileItemReader<PartyDataBean> sfpOrgItemreader() {
		FlatFileItemReader<PartyDataBean> itemReader = new FlatFileItemReader<PartyDataBean>();
		itemReader.setLineMapper(lineMapper());
		itemReader.setLinesToSkip(1);
		itemReader.setResource(new FileSystemResource("G:/Learnings/Projects/sfdcIntegration/sfp_org.txt"));
		return itemReader;
	}

	@Bean
	public LineMapper<PartyDataBean> lineMapper() {
		DefaultLineMapper<PartyDataBean> lineMapper = new DefaultLineMapper<PartyDataBean>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(" ");
		lineTokenizer.setNames(new String[] { "GridId", "partyName", "privInd", "partyStatus" });
		lineTokenizer.setIncludedFields(new int[] { 0, 1, 2, 3 });
		BeanWrapperFieldSetMapper<PartyDataBean> fieldSetMapper = new BeanWrapperFieldSetMapper<PartyDataBean>();
		fieldSetMapper.setTargetType(PartyDataBean.class);
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}

	@Bean
	public ItemWriter<PartyDataBean> csvwriter() {
		FlatFileItemWriter<PartyDataBean> itemWriter = new FlatFileItemWriter<>();

		String exportFileHeader = "GRIDID,PARTYNAME,PRIVIND,PARTYSTATUS";
		StringHeaderWriter headerWriter = new StringHeaderWriter(exportFileHeader);
		itemWriter.setHeaderCallback(headerWriter);

		String exportFilePath = "G:/Learnings/Projects/sfdcIntegration/partyData.csv";
		itemWriter.setResource(new FileSystemResource(exportFilePath));

		LineAggregator<PartyDataBean> lineAggregator = createStudentLineAggregator();
		itemWriter.setLineAggregator(lineAggregator);

		return itemWriter;
	}

	@Bean
	public JdbcBatchItemWriter<PartyDataBean> databasewriter() {
		JdbcBatchItemWriter<PartyDataBean> itemWriter = new JdbcBatchItemWriter<PartyDataBean>();
		itemWriter.setDataSource(dataSource());
		itemWriter.setSql(
				"INSERT INTO ACCOUNT (GRIDID, PARTYNAME, PRIVIND, PARTYSTATUS) VALUES (:GridId, :partyName, :privInd, :partyStatus)");
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<PartyDataBean>());
		return itemWriter;
	}

	@Bean
	public DataSource dataSource() {
		EmbeddedDatabaseBuilder embeddedDatabaseBuilder = new EmbeddedDatabaseBuilder();
		return embeddedDatabaseBuilder.addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
				.addScript("classpath:org/springframework/batch/core/schema-h2.sql").addScript("classpath:account.sql")
				.setType(EmbeddedDatabaseType.H2).build();
	}

	@Bean
	public CompositeItemWriter<PartyDataBean> compositeItemWriter() {
		List<ItemWriter<? super PartyDataBean>> writers = new ArrayList<>(2);
		writers.add(csvwriter());
		writers.add(databasewriter());

		CompositeItemWriter<PartyDataBean> itemWriter = new CompositeItemWriter<>();

		itemWriter.setDelegates(writers);

		return itemWriter;
	}

	private LineAggregator<PartyDataBean> createStudentLineAggregator() {
		DelimitedLineAggregator<PartyDataBean> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");

		FieldExtractor<PartyDataBean> fieldExtractor = createStudentFieldExtractor();
		lineAggregator.setFieldExtractor(fieldExtractor);

		return lineAggregator;
	}

	private FieldExtractor<PartyDataBean> createStudentFieldExtractor() {
		BeanWrapperFieldExtractor<PartyDataBean> extractor = new BeanWrapperFieldExtractor<>();
		extractor.setNames(new String[] { "GridId", "partyName", "privInd", "partyStatus" });
		return extractor;
	}

}
