package com.ing.sfdcIntegration.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
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
		lineTokenizer.setNames(new String[] { "GridId", "partyName", "privInd", "partyStatus", "Street", "FlatNo",
				"City", "CIBIL", "CreditScore", "Designation", "Role" });
		lineTokenizer.setIncludedFields(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
		BeanWrapperFieldSetMapper<PartyDataBean> fieldSetMapper = new BeanWrapperFieldSetMapper<PartyDataBean>();
		fieldSetMapper.setTargetType(PartyDataBean.class);
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}

	@Bean
	public ItemWriter<PartyDataBean> accountCsvwriter() {
		FlatFileItemWriter<PartyDataBean> itemWriter = new FlatFileItemWriter<>();

		String exportFileHeader = "GRIDID__C,PARTYNAME__C,PRIVIND__C,PARTYSTATUS__C";
		StringHeaderWriter headerWriter = new StringHeaderWriter(exportFileHeader);
		itemWriter.setHeaderCallback(headerWriter);

		String exportFilePath = "G:/Learnings/Projects/sfdcIntegration/Account.csv";
		itemWriter.setResource(new FileSystemResource(exportFilePath));

		LineAggregator<PartyDataBean> lineAggregator = createPartyDataLineAggregator("Account");
		itemWriter.setLineAggregator(lineAggregator);

		return itemWriter;
	}

	@Bean
	public ItemWriter<PartyDataBean> addressCsvwriter() {
		FlatFileItemWriter<PartyDataBean> itemWriter = new FlatFileItemWriter<>();

		String exportFileHeader = "STREET__C,FLATNO__C,CITY__C";
		StringHeaderWriter headerWriter = new StringHeaderWriter(exportFileHeader);
		itemWriter.setHeaderCallback(headerWriter);

		String exportFilePath = "G:/Learnings/Projects/sfdcIntegration/Address.csv";
		itemWriter.setResource(new FileSystemResource(exportFilePath));

		LineAggregator<PartyDataBean> lineAggregator = createPartyDataLineAggregator("Address");
		itemWriter.setLineAggregator(lineAggregator);

		return itemWriter;
	}

	@Bean
	public ItemWriter<PartyDataBean> clientRatingsCsvwriter() {
		FlatFileItemWriter<PartyDataBean> itemWriter = new FlatFileItemWriter<>();

		String exportFileHeader = "CIBIL__C,CREDITSCORE__C";
		StringHeaderWriter headerWriter = new StringHeaderWriter(exportFileHeader);
		itemWriter.setHeaderCallback(headerWriter);

		String exportFilePath = "G:/Learnings/Projects/sfdcIntegration/ClientRatings.csv";
		itemWriter.setResource(new FileSystemResource(exportFilePath));

		LineAggregator<PartyDataBean> lineAggregator = createPartyDataLineAggregator("ClientRatings");
		itemWriter.setLineAggregator(lineAggregator);

		return itemWriter;
	}

	@Bean
	public ItemWriter<PartyDataBean> relationshipManagersCsvwriter() {
		FlatFileItemWriter<PartyDataBean> itemWriter = new FlatFileItemWriter<>();

		String exportFileHeader = "DESIGNATION__C,ROLE__C";
		StringHeaderWriter headerWriter = new StringHeaderWriter(exportFileHeader);
		itemWriter.setHeaderCallback(headerWriter);

		String exportFilePath = "G:/Learnings/Projects/sfdcIntegration/RelationshipManagers.csv";
		itemWriter.setResource(new FileSystemResource(exportFilePath));

		LineAggregator<PartyDataBean> lineAggregator = createPartyDataLineAggregator("RelationshipManagers");
		itemWriter.setLineAggregator(lineAggregator);

		return itemWriter;
	}

	@Bean
	public CompositeItemWriter<PartyDataBean> compositeItemWriter() {
		List<ItemWriter<? super PartyDataBean>> writers = new ArrayList<>(5);
		writers.add(accountCsvwriter());
		writers.add(addressCsvwriter());
		writers.add(clientRatingsCsvwriter());
		writers.add(relationshipManagersCsvwriter());

		CompositeItemWriter<PartyDataBean> itemWriter = new CompositeItemWriter<>();

		itemWriter.setDelegates(writers);

		return itemWriter;
	}

	private LineAggregator<PartyDataBean> createPartyDataLineAggregator(String writerType) {
		DelimitedLineAggregator<PartyDataBean> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");

		FieldExtractor<PartyDataBean> fieldExtractor = createPartyDataFieldExtractor(writerType);
		lineAggregator.setFieldExtractor(fieldExtractor);

		return lineAggregator;
	}

	private FieldExtractor<PartyDataBean> createPartyDataFieldExtractor(String writerType) {
		BeanWrapperFieldExtractor<PartyDataBean> extractor = new BeanWrapperFieldExtractor<>();
		if ("Account".equals(writerType)) {
			extractor.setNames(new String[] { "GridId", "partyName", "privInd", "partyStatus" });
		} else if ("Address".equals(writerType)) {
			extractor.setNames(new String[] { "Street", "FlatNo", "City" });
		} else if ("ClientRatings".equals(writerType)) {
			extractor.setNames(new String[] { "CIBIL", "CreditScore" });
		} else if ("RelationshipManagers".equals(writerType)) {
			extractor.setNames(new String[] { "Designation", "Role" });
		} else {
		}

		return extractor;
	}

}
