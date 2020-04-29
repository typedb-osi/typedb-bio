from grakn.client import GraknClient
import csv 
import os

def coronavirusMigrator(uri, keyspace):
	client = GraknClient(uri=uri)
	session = client.session(keyspace=keyspace)
	tx = session.transaction().write()
	print('.....')
	print('Starting with Coronavirus file.')
	print('.....')

	# Temporary manual ingestion of locations
	graql = f"""insert $c isa country, has country-name 'China'; $c2 isa country, has country-name 'Kingdom of Saudi Arabia'; 
	$c3 isa country, has country-name 'USA'; $c4 isa country, has country-name 'South Korea';"""
	tx.query(graql)
	tx.commit()

	with open('../biograkn-covid/Dataset/Coronaviruses/Genome identity.csv', 'rt', encoding='utf-8') as csvfile:
		tx = session.transaction().write()
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
			data['coronavirus'] = i[1].strip()
			data['identity%'] = i[2]
			data['host'] = i[3][0:-1].strip()
			data['location-discovered'] = i[4][0:-1]
			import_file.append(data)
		for q in import_file: 
			print(q)
			graql = f"""match $c isa country, has country-name "{q['location-discovered']}"; 
			$o isa organism, has organism-name "{q['host']}";
			insert $v isa virus, has genbank-id "{q['genbank-id']}", 
			has virus-name "{q['coronavirus']}", 
			has identity-percentage "{q['identity%']}";
			$r (discovering-location: $c, discovered-virus: $v) isa discovery;
			$r1 (hosting-organism: $o, hosted-virus: $v) isa organism-virus-hosting;"""
			print(graql)
			tx.query(graql)
		tx.commit()


	with open('../biograkn-covid/Dataset/Coronaviruses/Host proteins (potential drug targets).csv', 'rt', encoding='utf-8') as csvfile:
		tx = session.transaction().write()
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
			data['uniprot-id'] = i[1].strip()
			data['entrez-id'] = i[2].strip()
			import_file.append(data)
		for q in import_file: 
			graql = f"""match $v isa virus, has virus-name "{q['coronavirus']}"; 
			$p isa protein, has uniprot-id "{q['uniprot-id']}";
			$g isa gene, has entrez-id "{q['entrez-id']}";
			insert $r2 (associated-virus-gene: $g, associated-virus: $v) isa gene-virus-association;
			$r3 (associated-virus-protein: $p, hosted-virus: $v) isa protein-virus-association;"""
			tx.query(graql)
			print(graql)
		tx.commit()
	print('.....')
	print('Finished with Coronavirus file.')
	print('.....')


