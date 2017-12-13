import regex as re
import pandas as pd





class GetData():
	
	def __init__(self):
		self.nodes=pd.read_csv('~/desktop/erisa_project/nodes.csv', encoding='utf-8')
		self.data=pd.read_csv('~/desktop/erisa_project/data.csv', encoding='utf-8',nrows=5)



class Advisory(GetData):

	def __init__(self):
		super().__init__()


	def process(self):
		
		for row in self.data.itertuples():
			self.content=row[3]
			self.id=row[1]
			self.title=row[2]
			print(self.title)
			self.pattern_opinion=r"(?<=Advisory Opinion|opinion|AO)[\s]+[\S]+[-][\S]+[\s]"
			self.pattern_opinions=r"(?<=Advisory Opinions)((\s+\S+-\S+)*\sand(\s+\S+-\S+))"
			self.opinion_text=list(set(re.findall(self.pattern_opinion, self.content)))
			#print(self.opinion_text)
			self.opinions_text=list(set(re.findall(self.pattern_opinions, self.content)))
			#print(self.opinions_text)
			self.opinion_items=[]
			for o in self.opinion_text:
				#o=str(o)
				o=o.strip()
				o=o.rstrip(".,;")
				self.opinion_items.append(o)
			#print(self.opinion_items)
			self.op_text=[]
			for t in self.opinions_text:
				self.op_text.append(t[0])
			#print(self.op_text)
			self.ops=[]
			for op in self.op_text:
				op=re.sub(' and', ',', op)
				self.ops=re.split(',', op)
			#print(self.ops)
			for p in self.ops:
				#p=str(p)
				p=p.strip()
				self.opinion_items.append(p)
			self.documents=self.nodes.loc[self.nodes['doc_type'] == 1827 ]
			self.dict_opinion=dict(zip(self.documents['nid'], self.documents['title']))
			self.dict_opinion_2={key:value for key, value in self.dict_opinion.items() if any(item in value for item in self.opinion_items)}
			#print(self.opinion_items)
			print(self.dict_opinion_2)
			self.data_opinion=pd.DataFrame(list(self.dict_opinion_2.items()), columns=['nid','title'])
			self.data_opinion['source_nid']=self.id
			self.data_opinion['source_title']=self.title
			print(self.data_opinion)
			if len(self.dict_opinion_2)>0:
				self.data_opinion.to_csv('~/desktop/erisa_project/final.csv',  mode='a', header=False, index=False)
			
			


class Section(GetData):
	
	def __init__(self):
		super().__init__()	


	def process(self):
		
		for row in self.data.itertuples():
			self.content=row[3]
			self.id=row[1]
			self.title=row[2]
			print(self.title)
			self.pattern_section=r"(?<=ERISA Section|ERISA section|ERISA \xA7)[\s]+[\S]+[\s]+|(?<=section|\xA7)[\s]+[\S]+[\s]+(?=of ERISA|of the ERISA)"
			self.section_text=list(set(re.findall(self.pattern_section, self.content)))
			self.pattern_code=r"(?<=Code Section|Code section|Code \xA7)[\s]+[\S]+[\s]+|(?<=section|\xA7)[\s]+[\S]+[\s]+(?=of the Code)"
			self.code_text=list(set(re.findall(self.pattern_code, self.content)))
			self.section_items=[]
			for s in self.section_text:
			#s=str(s)
				s=s.rstrip(' ,.:;?!')
				s=re.sub('[\(][a-zA-Z][\)].*', '', s)
				s='ERISA Section'+s+' '
				self.section_items.append(s)
			self.section_items=list(set(self.section_items))
			self.code_items=[]
			for c in self.code_text:
			#c=str(c)
				c=c.rstrip(' ,.:;?!')
				c=re.sub('[\(][a-zA-Z][\)].*', '', c)
				c='IRC 26 USC'+c
				self.code_items.append(c)
			self.code_items=list(set(self.code_items))
			self.statutes=self.nodes.loc[self.nodes['type'] == 'statute' ]
			self.dict_section=dict(zip(self.statutes['nid'], self.statutes['title']))
			self.dict_section_2={key:value for key, value in self.dict_section.items() if any(item in value for item in self.section_items)}
			self.dict_code={key:value for key, value in self.dict_section.items() if any(item in value for item in self.code_items)}
			self.data_section=pd.DataFrame(list(self.dict_section_2.items()), columns=['nid','title'])
			self.data_section['source_nid']=self.id
			self.data_section['source_title']=self.title
			print(self.data_section)
			if len(self.dict_section_2)>0:
				self.data_section.to_csv('~/desktop/erisa_project/final.csv',  mode='a', header=False, index=False)
			self.data_code=pd.DataFrame(list(self.dict_code.items()), columns=['nid','title'])
			self.data_code['source_nid']=self.id
			self.data_code['source_title']=self.title
			print(self.data_code)
			if len(self.dict_code)>0:
				self.data_code.to_csv('~/desktop/erisa_project/final.csv',  mode='a', header=False, index=False)
			




if __name__=="__main__":
	advisories=Advisory()
	advisories.process()
	sections=Section()
	sections.process()
	
