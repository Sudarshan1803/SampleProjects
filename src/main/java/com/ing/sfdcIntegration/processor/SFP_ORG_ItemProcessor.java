package com.ing.sfdcIntegration.processor;

import org.springframework.batch.item.ItemProcessor;

import com.ing.sfdcIntegration.bean.PartyDataBean;

public class SFP_ORG_ItemProcessor implements ItemProcessor<PartyDataBean, PartyDataBean> {

	@Override
	public PartyDataBean process(PartyDataBean item) throws Exception {

		if ("Y".equals(item.getPrivInd()) || "INA".equals(item.getPartyStatus())) {
			return null;
		}

		System.out.println("Inserting Account : " + item);
		return item;
	}

}
