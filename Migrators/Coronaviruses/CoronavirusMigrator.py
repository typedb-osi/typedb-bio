import csv
from grakn.client import GraknClient, SessionType, TransactionType


def coronavirusMigrator(uri, database):
	client = GraknClient.core(uri)
	session = client.session(database, SessionType.DATA)
	tx = session.transaction(TransactionType.WRITE)
	print('.....')
	print('Starting with Coronavirus file.')
	print('.....')

	# Temporary manual ingestion of locations
	graql = f"""insert $c isa country, has country-name 'China'; $c2 isa country, has country-name 'Kingdom of Saudi Arabia'; 
	$c3 isa country, has country-name 'USA'; $c4 isa country, has country-name 'South Korea'; $o isa organism, has organism-name 'Mouse';"""
	tx.query().insert(graql)
	tx.commit()
	tx.close()

	with open('Dataset/Coronaviruses/Genome identity.csv', 'rt', encoding='utf-8') as csvfile:
		tx = session.transaction(TransactionType.WRITE)
		csvreader = csv.reader(csvfile, delimiter=',')
		raw_file = []
		n = 0
		for row in csvreader: 
			n = n + 1
			if n is not 1:
				raw_file.append(row)
		import_file = []
		for i in raw_file:
			data = {}
			data['genbank-id'] = i[0]
			data['identity%'] = i[2]
			data['host'] = i[3][0:-1].strip()
			data['location-discovered'] = i[4].strip()
			data['coronavirus-1'] = i[1].strip()
			try: 
				data['coronavirus-2'] = i[5].strip()
			except Exception: 
				pass
			try: 
				data['coronavirus-3'] = i[6].strip()
			except Exception:
				pass
			import_file.append(data)
		for q in import_file: 
			virus_name = ""
			try: 
				virus_name = f""" has virus-name "{q['coronavirus-1']}", has virus-name "{q['coronavirus-2']}", has virus-name "{q['coronavirus-3']}", """
			except Exception:
				try: 
					virus_name = f""" has virus-name "{q['coronavirus-1']}", has virus-name "{q['coronavirus-2']}", """
				except Exception: 
					virus_name = f""" has virus-name "{q['coronavirus-1']}", """
			print(virus_name)
			graql = f"""match $c isa country, has country-name "{q['location-discovered']}"; 
			$o isa organism, has organism-name "{q['host']}";
			insert $v isa virus, has genbank-id "{q['genbank-id']}", {virus_name}
			has identity-percentage "{q['identity%']}";
			$r (discovering-location: $c, discovered-virus: $v) isa discovery;
			$r1 (hosting-organism: $o, hosted-virus: $v) isa organism-virus-hosting;"""
			print(graql)
			tx.query().insert(graql)
		tx.commit()
		tx.close()


	with open('Dataset/Coronaviruses/Host proteins (potential drug targets).csv', 'rt', encoding='utf-8') as csvfile:
		tx = session.transaction(TransactionType.WRITE)
		csvreader = csv.reader(csvfile, delimiter=',')
		raw_file = []
		n = 0
		for row in csvreader: 
			n = n + 1
			if n is not 1:
				raw_file.append(row)
		import_file = []
		for i in raw_file:
			data = {}
			data['coronavirus'] = i[0].strip()
			data['uniprot-id'] = i[3].strip()
			data['entrez-id'] = i[4].strip()
			import_file.append(data)
		for q in import_file: 
			graql = f"""match $v isa virus, has virus-name "{q['coronavirus']}"; 
			$p isa protein, has uniprot-id "{q['uniprot-id']}";
			$g isa gene, has entrez-id "{q['entrez-id']}";
			insert $r2 (associated-virus-gene: $g, associated-virus: $v) isa gene-virus-association;
			$r3 (hosting-virus-protein: $p, associated-virus: $v) isa protein-virus-association;"""
			tx.query().insert(graql)
			print(graql)
		tx.commit()
		tx.close()
	print('.....')
	print('Finished with Coronavirus file.')
	print('.....')


