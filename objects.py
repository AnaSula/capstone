import regex as re
import pandas as pd





class GetData(): #this class imports data that was downloaded from the Solr index
	
	def __init__(self):
		self.nodes=pd.read_csv('~/desktop/erisa_project/nodes.csv', encoding='utf-8', dtype='object')
		self.data=pd.read_csv('~/desktop/erisa_project/data.csv', encoding='utf-8')



class Advisory(GetData): #this class processes document type: Advisory Opinions

	def __init__(self):
		super().__init__()


	def process(self):
		
		for row in self.data.itertuples():
			self.content=row[3]
			self.id=row[1]
			self.title=row[2]
			self.pattern_opinion=r"(?<=Advisory Opinion|opinion|AO)[\s]+[\S]+[-][\S]+[\s]"
			self.pattern_opinion_pl=r"(?<=Advisory Opinions)((\s+\S+-\S+)*\sand(\s+\S+-\S+))"
			self.opinion_text=list(set(re.findall(self.pattern_opinion, self.content)))
			self.opinion_text_pl=list(set(re.findall(self.pattern_opinion_pl, self.content)))
			self.opinion_items=[]
			for o in self.opinion_text:
				o=o.strip()
				o=o.rstrip(".,;")
				self.opinion_items.append(o)
			self.op_text=[]
			for t in self.opinion_text_pl:
				self.op_text.append(t[0])
			self.ops=[]
			for op in self.op_text:
				op=re.sub(' and', ',', op)
				self.ops=re.split(',', op)
			for p in self.ops:
				p=p.strip()
				if p!='':
					self.opinion_items.append(p)
			self.documents=self.nodes.loc[self.nodes['doc_type'] == '1827' ]
			self.dict_opinion=dict(zip(self.documents['nid'], self.documents['title']))
			self.dict_opinion_2={key:value for key, value in self.dict_opinion.items() if any(item in value for item in self.opinion_items)}
			self.data_opinion=pd.DataFrame(list(self.dict_opinion_2.items()), columns=['des_nid','_des_title'])
			self.data_opinion['source_nid']=self.id
			self.data_opinion['source_title']=self.title
			self.data_opinion['field_name']='field_general_entity_reference'
			if len(self.dict_opinion_2)>0:
				self.data_opinion.to_csv('~/desktop/erisa_project/final.csv',  mode='a', header=False, index=False)
			
			


class Section(GetData): #this class processes document type: Statutes
	
	def __init__(self):
		super().__init__()	


	def process(self):
		
		for row in self.data.itertuples():
			self.content=row[3]
			self.id=row[1]
			self.title=row[2]
			self.pattern_section=r"(?<=ERISA Section|ERISA section|ERISA \xA7)[\s]+[\S]+[\s]+|(?<=section|\xA7|Section)[\s]+[\S]+[\s]+(?=of ERISA|of the ERISA)"
			self.section_text=list(set(re.findall(self.pattern_section, self.content)))
			self.pattern_code=r"(?<=Code Section|Code section|Code \xA7)[\s]+[\S]+[\s]+|(?<=section|\xA7)[\s]+[\S]+[\s]+(?=of the Code|of the Internal Revenue Code)"
			self.code_text=list(set(re.findall(self.pattern_code, self.content)))
			self.section_items=[]
			for s in self.section_text:
				s=s.rstrip(' ,.:;?!')
				s=re.sub('[\(][a-zA-Z][\)].*', '', s)
				s='ERISA Section'+s+' '
				self.section_items.append(s)
			self.section_items=list(set(self.section_items))
			self.code_items=[]
			for c in self.code_text:
				c=c.rstrip(' ,.:;?!')
				c=re.sub('[\(][a-zA-Z][\)].*', '', c)
				c='IRC 26 USC'+c+' '
				self.code_items.append(c)
			self.code_items=list(set(self.code_items))
			self.statutes=self.nodes.loc[self.nodes['type'] == 'statute' ]
			self.dict_section=dict(zip(self.statutes['nid'], self.statutes['title']))
			self.dict_section_2={key:value for key, value in self.dict_section.items() if any(item in value for item in self.section_items)}
			self.dict_code={key:value for key, value in self.dict_section.items() if any(item in value for item in self.code_items)}
			self.data_section=pd.DataFrame(list(self.dict_section_2.items()), columns=['nid','title'])
			self.data_section['source_nid']=self.id
			self.data_section['source_title']=self.title
			self.data_section['field_name']='field_affected_statutes'
			if len(self.dict_section_2)>0:
				self.data_section.to_csv('~/desktop/erisa_project/final.csv',  mode='a', header=False, index=False)
			self.data_code=pd.DataFrame(list(self.dict_code.items()), columns=['nid','title'])
			self.data_code['source_nid']=self.id
			self.data_code['source_title']=self.title
			self.data_code['field_name']='field_affected_statutes'
			if len(self.dict_code)>0:
				self.data_code.to_csv('~/desktop/erisa_project/final.csv',  mode='a', header=False, index=False)




class Rule(GetData): #this class processes document type: Rules

	def __init__(self):
		super().__init__()



	def process(self):
		for row in self.data.itertuples():
			self.content=row[3]
			self.id=row[1]
			self.title=row[2]
			self.pattern_reg=r"(?<=29 CFR|29 C\.F\.R\.|29 CFR \xA7|29 C\.F\.R\. \xA7)[\s]+[25][\d]{3}.[\d]+[\w]*-[\s]*[\d]+"
			self.reg_text=list(set(re.findall(self.pattern_reg, self.content)))
			self.reg_items=[]
			for r in self.reg_text:
				r=r+':'
				self.reg_items.append(r)
			self.reg_items=list(set(self.reg_items))
			self.rules=self.nodes.loc[self.nodes['type'] == 'rule' ]
			self.dict_rules=dict(zip(self.rules['nid'], self.rules['title']))
			self.dict_rules_2={key:value for key, value in self.dict_rules.items() if any(item in value for item in self.reg_items)}
			self.data_rule=pd.DataFrame(list(self.dict_rules_2.items()), columns=['nid','title'])
			self.data_rule['source_nid']=self.id
			self.data_rule['source_title']=self.title
			self.data_rule['field_name']='field_affected_rules'
			if len(self.dict_rules_2)>0:
				self.data_rule.to_csv('~/desktop/erisa_project/final.csv',  mode='a', header=False, index=False)



			

class PTE(GetData): #this class processes document type: Prohibited Transaction Enforecemtns

	def __init__(self):
		super().__init__()



	def process(self):
		for row in self.data.itertuples():
			self.content=row[3]
			self.id=row[1]
			self.title=row[2]
			self.pattern_pte=r"PTE[\s]+[\d]+-[\s]*[\d]+"
			self.pte_text=list(set(re.findall(self.pattern_pte, self.content)))
			self.pte_items=[]
			for p in self.pte_text:
				p=p.rstrip(' ,.:;?!')
				self.pte_items.append(p)
			self.pte_items=list(set(self.pte_items))
			self.pte_docs=self.nodes.loc[self.nodes['type'] == 'document' ]
			self.dict_ptes=dict(zip(self.pte_docs['nid'], self.pte_docs['title']))
			self.dict_ptes_2={key:value for key, value in self.dict_ptes.items() if any(item in value for item in self.pte_items)}
			self.data_pte=pd.DataFrame(list(self.dict_ptes_2.items()), columns=['nid','title'])
			self.data_pte['source_nid']=self.id
			self.data_pte['source_title']=self.title
			self.data_pte['field_name']='field_general_entity_reference'
			if len(self.dict_ptes_2)>0:
				self.data_pte.to_csv('~/desktop/erisa_project/final.csv',  mode='a', header=False, index=False)




class FederalRegister(GetData): #this class processes document type: Federal Register Releases

	def __init__(self):
		super().__init__()



	def process(self):
		for row in self.data.itertuples():
			self.content=row[3]
			self.id=row[1]
			self.title=row[2]
			self.pattern_fr=r"\d+\s+FR\s+\d+|\d+\s+F\.R\.\s+\d+|\d+\s+Fed\. Reg\.\s+\d+|\d+\s+Federal Register\s+\d+|\d+\s+FR\.\s+\d+"
			self.fr_text=list(set(re.findall(self.pattern_fr, self.content)))
			self.fr_items=[]
			for f in self.fr_text:
				f=f.rstrip(' ,.:;?!')
				f=re.sub('\D+', ' FR ', f)
				self.fr_items.append(f)
			self.frs=self.nodes.loc[self.nodes['type']=='federal_register_release']
			self.fr_items=list(set(self.fr_items))
			self.dict_fr=dict(zip(self.frs['nid'], self.frs['title']))
			self.dict_fr_2={key:value for key, value in self.dict_fr.items() if any(item in value for item in self.fr_items)}
			self.data_fr=pd.DataFrame(list(self.dict_fr_2.items()), columns=['nid','title'])
			self.data_fr['source_nid']=self.id
			self.data_fr['source_title']=self.title
			self.data_fr['field_name']='field_general_entity_reference'
			if len(self.dict_fr_2)>0:
				self.data_fr.to_csv('~/desktop/erisa_project/final.csv',  mode='a', header=False, index=False)






if __name__=="__main__":
	advisories=Advisory()
	advisories.process()
	sections=Section()
	sections.process()
	rules=Rule()
	rules.process()
	ptes=PTE()
	ptes.process()
	frs=FederalRegister()
	frs.process()
	
	
