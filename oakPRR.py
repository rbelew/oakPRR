'''oakPRR: analysis of Oakland's Public Record Requests
Created on Mar 25, 2021

@author: rik, sdoran
'''

from collections import defaultdict
import csv
import datetime
import json
import math
import os
import pytz
import re
import sqlite3 as sqlite
import sys

## CONSTANTS

# NextRequestAPIV2 dateTime format
# 2013-06-12T00:00:00.000-07:00
NRDTformat = '%Y-%m-%dT%H:%M:%S.%f%z'

CSVDTFormat2 = '%m/%d/%y %H:%M'
# 2/17/16 0:00

OaklandTimeZone = pytz.timezone('America/Los_Angeles')

PRRdb_fields = {
	'prr': {
		'id':  'INTEGER' ,
		'pretty_id': 'TEXT',
		'created_at':  'TEXT',
		'updated_at':  'TEXT',
		'request_text':  'TEXT',
		'request_date':  'TEXT',
		'due_date':  'TEXT', 
		'closed_date':  'TEXT',
		'publish_date':  'TEXT',
		'visibility':  'TEXT',
		'closure_reasons':  'TEXT',
		'prr_state':  'TEXT',
		'initial_contact_date': 'TEXT',
		'initial_contact_event_id': 'INTEGER',

		# compliance	None
		# initial_due_date	None
		
		'poc_id':  'INTEGER',
		
		# 'expiration_date':  'TEXT',
		# 'account_id':  'INTEGER', 210331: 90 for Oakland
		# 'anticipated_fulfilled_at':  'TEXT',
		
		# demo	False
		# jlarc_response_biz_days	None
		# biz_days_to_close	None
		# days_to_close	None
		
		'general_report_response_days':  'INTEGER',
		'ever_overdue':  'BOOLEAN',

		# bdp_used	False
		
		},

	'event': {
		'id':  'INTEGER',
		'request_id':  'INTEGER',
		'event_type_id':  'INTEGER',
		'subject_user_id':  'INTEGER',
		# 'object_user_id':  'INTEGER', 210331:  never used
		'description':  'TEXT',
		'created_at':  'TEXT',
		'updated_at':  'TEXT',
		'byline':  'TEXT',
		'event_state':  'TEXT',
		#'account_id':  'INTEGER', 210331: 90 for Oakland
		'deleted':  'BOOLEAN'
		},

	'note': {
		'id':  'INTEGER',
		'note_text':  'TEXT',
		'created_at':  'TEXT',
		'updated_at':  'TEXT',
		'request_id':  'INTEGER',
		'email':  'BOOLEAN',
		'deleted':  'BOOLEAN',	
		'user_id':  'INTEGER',	
		'note_state':  'TEXT',
		# 'account_id':  'INTEGER' 210331: 90 for Oakland
		},

	'document': {
		'id':  'INTEGER',
		'title':  'TEXT',
		'url':   'TEXT', 
		'description':  'TEXT',
		'created_at':  'TEXT',
		'updated_at':  'TEXT',
		'count':  'INTEGER',
		'doc_date':  'TEXT',
		# 'redacted_at':  'TEXT', 210331: NEVER USED 
		'link':  'BOOLEAN',
		'requester_upload':  'BOOLEAN',
		'document_state':  'TEXT',
		
		# archived	False
		# expiration_date	None
		# exempt_from_retention	False
		# subfolder	
		# token	None
		# token_expires_at	None
		# demo	False
		# temp_upload_key	None
		# attachment_via_email	False
		# original_doc_link	None

		'filename':  'TEXT',
		'request_id':  'INTEGER',
		'review_state':  'TEXT',
		# 'account_id':  'INTEGER', 210331: 90 for Oakland
		'attachment_via_email':  'BOOLEAN',
		'original_doc_link':  'TEXT'
		},
	
	'department': {
		'id': 'INTEGER',
		'name': 'TEXT',
		'description': 'TEXT'
	},
	
	'depreq': {
		'id': 'INTEGER',
		'request_id': 'INTEGER',
		'department_id': 'INTEGER'
	},

	'event_type': {	
		'id': 'INTEGER',
		'name': 'TEXT',
		'state': 'TEXT',
		'display_on_request_timeline': 'BOOLEAN',
		'category': 'TEXT'
	},
	
	'message_templates': {	
		'id': 'INTEGER',
		'name': 'TEXT',
		'description': 'TEXT',
		'created_at': 'TEXT',
		'initial_contact': 'BOOLEAN',
		# 'account_id': 'INTEGER', 90 for Oakland
		'category_id': 'TEXT'
	},
	
	'notes_message_templates': {	
		'id': 'INTEGER',
		'note_id': 'INTEGER',
		'message_template_id': 'INTEGER',
		'created_at': 'TEXT',
		# 'account_id': 'INTEGER' 90 for Oakland
	}
	
}

## UTILITIES

def basicStats(l):
	"Returns avg and stdev"
	if len(l) == 0:
		return(0.,0.)

	tot = 0
	for n in l:
		tot += n
	avg = float(tot) / len(l)

	sumDiffSq = 0.
	for n in l:
		sumDiffSq += (n-avg)*(n-avg)

	stdev = math.sqrt(sumDiffSq) / float(len(l))
	return (avg,stdev)

def initPRRdb(currDB):

	curs = currDB.cursor()

	for tblName in PRRdb_fields.keys():
		curs.execute('DROP TABLE IF EXISTS %s' % (tblName))
		cmd = "CREATE TABLE IF NOT EXISTS %s (\n" % (tblName)

		# flds = ',\n'.join(['%s %s' % (fld,PRRdb_fields[tblName][fld]) for fld in PRRdb_fields[tblName]])
		fldList = []
		for fld in PRRdb_fields[tblName]:
			fstr = f'{fld} {PRRdb_fields[tblName][fld]}'
			if fld=='id':
				fstr += ' PRIMARY KEY'
			fldList.append(fstr)
		flds = ',\n'.join(fldList)
		
		cmd += flds
		cmd += ")\n"
		curs.execute(cmd)

	# add indices on prr's request_id
	for tblName in ['event','note','document','depreq']:
		cmd = f'create index {tblName}_prrIdx on {tblName}(request_id)'
		curs.execute(cmd)
		
	# check to make sure it loaded as expected
	curs = currDB.cursor()
	curs.execute("select tbl_name from sqlite_master")
	allTblList = curs.fetchall()
	for tblName in PRRdb_fields.keys():
		assert (tblName,) in allTblList, "initPRRdb: no %s table?!" % tblName

	return currDB

def bldPRRdb(jsonDir,startDate=None,endDate=None):

	dbfile = jsonDir +  'prr.db'
	
	if os.path.exists(dbfile):
		os.remove(dbfile)
		
	currDB = sqlite.connect(dbfile)
	currDB.isolation_level = None
	
	initPRRdb(currDB)

	ninsertPerTrans = 1000

	cursor = currDB.cursor()
	
	## load main PRR
	jfile = jsonDir+'OakPRR_all.json'
	prrList = json.load(open(jfile))
	
	nolder = 0
	nrecent = 0
	ncreateB4req = 0
	nnew = 0
	reqid2dbidx = {}  # to make filtering of events,notes,docs more efficient
	
	cursor.execute('begin')
	for prr in prrList:

		if prr['created_at'] == None:
			nolder += 1
			continue

		createDate = datetime.datetime.strptime(prr['created_at'],NRDTformat)
		reqdate = datetime.datetime.strptime(prr['request_date'],NRDTformat)
		if createDate<reqdate:
			ncreateB4req += 1
		minDate = min(createDate,reqdate)
		maxDate = max(createDate,reqdate)
		if startDate != None and minDate < startDate:
			nolder += 1
			continue
		if endDate != None and maxDate >= endDate:
			nrecent += 1
			continue
		
		nnew += 1
		rawFldList = list(PRRdb_fields['prr'].keys())
		# NB: "state" in json dict must be renamed as it is reserved word in SQL
		modList = [f if f != 'state' else 'prr_state' for f in rawFldList]
		fldList = sorted(modList)		
		
		flds = ','.join(fldList)
		qms = ','.join(len(fldList)*'?')
		sql = 'insert into prr (%s) values (%s)' % (flds,qms)

		valList = [prr[f] if f != 'prr_state' else prr['state'] for f in fldList]
		
		cursor.execute(sql,tuple(valList))
		prrIdx = cursor.lastrowid
		
		reqid = prr['id']
		reqid2dbidx[reqid] = prrIdx
		
		if nnew % ninsertPerTrans == 0:
			cursor.execute('commit')
			cursor.execute('begin')
			# print(f'\t bldPRRdb: row {nnew} nolder={nolder} nrecent={nrecent} prrIdx={prrIdx}')

	cursor.execute('commit')
	cmd = 'select count(*) from prr'
	cursor.execute(cmd)
	nprr = cursor.fetchone()[0]
	print(f'bldPRRdb: PRR done NPRR={nprr} nolder={nolder} nrecent={nrecent} ncreateB4req={ncreateB4req} prrIdx={prrIdx}')
	
	## attach EVENTS related newer PRR
	jfile = jsonDir+'events.json'
	eventList = json.load(open(jfile))
	
	nskip = 0
	nmissReq = 0
	nnew = 0
	cursor.execute('begin')
	for event in eventList:
		if 'request_id' not in event:
			# print('huh')
			nmissReq += 1
			continue
		
		reqid = event['request_id']
		if reqid not in reqid2dbidx:
			nskip += 1
			continue
		
		nnew += 1
		rawFldList = list(PRRdb_fields['event'].keys())
		# NB: "state" in json dict must be renamed as it is reserved word in SQL
		modList = [f if f != 'state' else 'event_state' for f in rawFldList]
		fldList = sorted(modList)		
		
		flds = ','.join(fldList)
		qms = ','.join(len(fldList)*'?')
		sql = 'insert into event (%s) values (%s)' % (flds,qms)

		valList = [event[f] if f != 'event_state' else event['state'] for f in fldList]
		
		cursor.execute(sql,tuple(valList))
		eventIdx = cursor.lastrowid
		
		if nnew % ninsertPerTrans == 0:
			cursor.execute('commit')
			cursor.execute('begin')
			# print(f'\t bldPRRdb: event row {nnew} eventIdx={eventIdx}')

	cursor.execute('commit')
	cmd = 'select count(*) from event'
	cursor.execute(cmd)
	nevent = cursor.fetchone()[0]
	print(f'bldPRRdb: Event done NEvent={nevent} nskip={nskip} eventIdx={eventIdx}')
		
	## attach DOCUMENTS related newer PRR
	jfile = jsonDir+'documents.json'
	documentList = json.load(open(jfile))
	
	nskip = 0
	nmissReq = 0
	nnew = 0
	missPRR = []
	cursor.execute('begin')
	for document in documentList:
		if 'request_id' not in document:
			# print('huh')
			nmissReq += 1
			continue
		
		reqid = document['request_id']
		
		# 210415: check for docs without attending PRR; maintain these in DB
		noPRR = False
		try:
			reqidInt = int(reqid)
		except:
			missPRR.append(document['id'])
			noPRR = True
		
		if reqid not in reqid2dbidx and not noPRR:
			nskip += 1
			continue
		
		nnew += 1
		rawFldList = list(PRRdb_fields['document'].keys())
		# NB: "state" in json dict must be renamed as it is reserved word in SQL
		modList = [f if f != 'state' else 'document_state' for f in rawFldList]
		fldList = sorted(modList)		
		
		flds = ','.join(fldList)
		qms = ','.join(len(fldList)*'?')
		sql = 'insert into document (%s) values (%s)' % (flds,qms)

		# 210415: 2do: focus only on state=PUBLIC documents?

		valList = [document[f] if f != 'document_state' else document['state'] for f in fldList]
		
		cursor.execute(sql,tuple(valList))
		documentIdx = cursor.lastrowid
		
		if nnew % ninsertPerTrans == 0:
			cursor.execute('commit')
			cursor.execute('begin')
			# print(f'\t bldPRRdb: document row {nnew} nskip={nskip} documentIdx={documentIdx}')

	cursor.execute('commit')
	cmd = 'select count(*) from document'
	cursor.execute(cmd)
	ndoc = cursor.fetchone()[0]
	print(f'bldPRRdb: Document done NDoc={ndoc} nskip={nskip} documentIdx={documentIdx} NDoc w/o PRR={len(missPRR)}')

	outf = jsonDir + 'docID-missPRR.csv'
	outs = open(outf,'w')
	outs.write('docID\n')
	for docID in missPRR:
		outs.write(f'{docID}\n')
	outs.close()
		
	## attach NOTES related newer PRR
	jfile = jsonDir+'notes.json'
	noteList = json.load(open(jfile))
	
	nskip = 0
	nmissReq = 0
	nnew = 0
	cursor.execute('begin')
	for note in noteList:
		if 'request_id' not in note:
			# print('huh')
			nmissReq += 1
			continue
		
		reqid = note['request_id']
		if reqid not in reqid2dbidx:
			nskip += 1
			continue
		
		nnew += 1
		rawFldList = list(PRRdb_fields['note'].keys())
		# NB: "state" in json dict must be renamed as it is reserved word in SQL
		modList = [f if f != 'state' else 'note_state' for f in rawFldList]
		fldList = sorted(modList)		
		
		flds = ','.join(fldList)
		qms = ','.join(len(fldList)*'?')
		sql = 'insert into note (%s) values (%s)' % (flds,qms)

		valList = [note[f] if f != 'note_state' else note['state'] for f in fldList]
		
		cursor.execute(sql,tuple(valList))
		noteIdx = cursor.lastrowid
		
		if nnew % ninsertPerTrans == 0:
			cursor.execute('commit')
			cursor.execute('begin')
			# print(f'\t bldPRRdb: note row {nnew} nskip={nskip} noteIdx={noteIdx}')
	
	cursor.execute('commit')	
	cmd = 'select count(*) from note'
	cursor.execute(cmd)
	nnote = cursor.fetchone()[0]
	print(f'bldPRRdb: Note done NNote={nnote} nskip={nskip} noteIdx={noteIdx}')

	## get all DEPARTMENTS
	jfile = jsonDir+'departments.json'
	departmentList = json.load(open(jfile))
	
	nskip = 0
	nmissReq = 0
	cursor.execute('begin')
	for ni,department in enumerate(departmentList):
				
		rawFldList = list(PRRdb_fields['department'].keys())
		fldList = sorted(rawFldList)		
		
		flds = ','.join(fldList)
		qms = ','.join(len(fldList)*'?')
		sql = 'insert into department (%s) values (%s)' % (flds,qms)

		valList = [department[f]  for f in fldList]
		
		cursor.execute(sql,tuple(valList))
		departmentIdx = cursor.lastrowid
	cursor.execute('commit')					
	cmd = 'select count(*) from department'
	cursor.execute(cmd)
	ndept = cursor.fetchone()[0]
	print(f'bldPRRdb: Department done NDept={ndept} departmentIdx={departmentIdx}')

	## attach departments_requests related newer PRR
	jfile = jsonDir+'departments_requests.json'
	depreqList = json.load(open(jfile))
	
	nskip = 0
	nmissReq = 0
	nnew = 0
	cursor.execute('begin')
	for depreq in depreqList:
		
		if 'request_id' not in depreq:
			# print('huh')
			nmissReq += 1
			continue

		# 210415: Only consider depreq with deleted=false
		if depreq['deleted'] == True:
			nskip += 1
			continue
		
		reqid = depreq['request_id']
		if reqid not in reqid2dbidx:
			nskip += 1
			continue
		
		nnew += 1
		rawFldList = list(PRRdb_fields['depreq'].keys())
		fldList = sorted(rawFldList)		
		
		flds = ','.join(fldList)
		qms = ','.join(len(fldList)*'?')
		sql = 'insert into depreq (%s) values (%s)' % (flds,qms)

		valList = [depreq[f] for f in fldList]
		
		cursor.execute(sql,tuple(valList))
		depreqIdx = cursor.lastrowid
		
		if ni % ninsertPerTrans == 0:
			cursor.execute('commit')
			cursor.execute('begin')
			# print(f'\t bldPRRdb: depreq row {ni} nskip={nskip} depreqIdx={depreqIdx}')
	
	cursor.execute('commit')		
	cmd = 'select count(*) from depreq'
	cursor.execute(cmd)
	ndepreq = cursor.fetchone()[0]
	print(f'bldPRRdb: depreq done NDepReq={ndepreq} nskip={nskip} depreqIdx={depreqIdx}')

	# 210331: add  event_type, message_template, notes_message_templates

	## attach EVENT_TYPE
	jfile = jsonDir+'event_types.json'
	etypeList = json.load(open(jfile))
	
	nmissReq = 0
	nnew = 0
	cursor.execute('begin')
	for etype in etypeList:
		
		nnew += 1
		rawFldList = list(PRRdb_fields['event_type'].keys())
		fldList = sorted(rawFldList)		
		
		flds = ','.join(fldList)
		qms = ','.join(len(fldList)*'?')
		sql = 'insert into event_type (%s) values (%s)' % (flds,qms)

		valList = [etype[f] for f in fldList]
		
		cursor.execute(sql,tuple(valList))
		etypeIdx = cursor.lastrowid
		
		if nnew % ninsertPerTrans == 0:
			cursor.execute('commit')
			cursor.execute('begin')
			# print(f'\t bldPRRdb: etype row {nnew} etypeIdx={etypeIdx}')
	
	cursor.execute('commit')	
	cmd = 'select count(*) from event_type'
	cursor.execute(cmd)
	netype = cursor.fetchone()[0]
	print(f'bldPRRdb: Event_type done NEventType={netype} etypeIdx={etypeIdx}')
	
	## attach MESSAGE_TEMPLATE
	jfile = jsonDir+'message_templates.json'
	msgtmpList = json.load(open(jfile))
	
	nmissReq = 0
	nnew = 0
	cursor.execute('begin')
	for msgtmp in msgtmpList:
		
		nnew += 1
		rawFldList = list(PRRdb_fields['message_templates'].keys())
		fldList = sorted(rawFldList)		
		
		flds = ','.join(fldList)
		qms = ','.join(len(fldList)*'?')
		sql = 'insert into message_templates (%s) values (%s)' % (flds,qms)

		valList = [msgtmp[f] for f in fldList]
		
		cursor.execute(sql,tuple(valList))
		msgtmpIdx = cursor.lastrowid
		
		if nnew % ninsertPerTrans == 0:
			cursor.execute('commit')
			cursor.execute('begin')
			# print(f'\t bldPRRdb: msgtmp row {nnew} msgtmpIdx={msgtmpIdx}')
	
	cursor.execute('commit')	
	cmd = 'select count(*) from message_templates'
	cursor.execute(cmd)
	nmtmp = cursor.fetchone()[0]
	print(f'bldPRRdb: msgtmp NMsgTemplate={nmtmp} done  msgtmpIdx={msgtmpIdx}')

	## attach NOTE_TEMPLATE
	jfile = jsonDir+'notes_message_templates.json'
	notetempList = json.load(open(jfile))
	
	nmissReq = 0
	nnew = 0
	cursor.execute('begin')
	for notetemp in notetempList:
		
		nnew += 1
		rawFldList = list(PRRdb_fields['notes_message_templates'].keys())
		fldList = sorted(rawFldList)		
		
		flds = ','.join(fldList)
		qms = ','.join(len(fldList)*'?')
		sql = 'insert into notes_message_templates (%s) values (%s)' % (flds,qms)

		valList = [notetemp[f] for f in fldList]
		
		cursor.execute(sql,tuple(valList))
		notetempIdx = cursor.lastrowid
		
		if nnew % ninsertPerTrans == 0:
			cursor.execute('commit')
			cursor.execute('begin')
			# print(f'\t bldPRRdb: notetemp row {nnew} notetempIdx={notetempIdx}')
	
	cursor.execute('commit')	
	cmd = 'select count(*) from notes_message_templates'
	cursor.execute(cmd)
	nnmsg = cursor.fetchone()[0]
	print(f'bldPRRdb: notetemp done NNoteMsg={nnmsg} notetempIdx={notetempIdx}')

def bldIndexTblCSV(inf,startDate=None,endDate=None):
	'''210416:  return prrIDTbl ONLY 
				make consistent with bldPRRdb
	'''

	prrTbl = {}
	statusTbl = defaultdict(int)
	reqCoTbl = defaultdict(list)
	ncloseDate = 0
	nolder = 0
	nrecent = 0
	nmultDept = 0
	nmissDept = 0
	deptSepChar = b'\xef\xbf\xbd' # only used in Finance
	FinanceEmChar = '�'
	EmChar = '—'
	
	reader = csv.DictReader(open(inf,encoding = "utf8",errors='replace'))
	for i,entry in enumerate(reader):
		prr = {}
		prrID = entry['Id']
		prr['id'] = prrID

		createDateStr = entry['Created At'].strip()
		if createDateStr == '':
			createDate = None
		else:
			createDate = datetime.datetime.strptime(createDateStr,CSVDTFormat2)
			createDate = createDate.replace(tzinfo=OaklandTimeZone)
			
		prr['createDate'] = createDate
		
		reqDateStr = entry['Request Date'].strip()
		if reqDateStr == '':
			reqdate = None
		else:
			reqdate = datetime.datetime.strptime(reqDateStr,CSVDTFormat2)
			reqdate = reqdate.replace(tzinfo=OaklandTimeZone)
		
		minDate = min(createDate,reqdate)
		maxDate = max(createDate,reqdate)
		if startDate != None and minDate < startDate:
			nolder += 1
			continue
		if endDate != None and maxDate >= endDate:
			nrecent += 1
			continue
		
		deptStr = entry['Departments'].strip()
		# NB: multiple department separated by semi-colon
		if deptStr.find(';') == -1:
			if deptStr == '':
				deptList = []
				nmissDept += 1
			else:
				deptList = [deptStr]
		else:
			nmultDept += 1
			deptList = [dept.strip() for dept in deptStr.split(';')]
		
		# Normalize department names	
		deptList2 = []
		for dept in deptList:
			dept = dept.replace(FinanceEmChar,EmChar)
			ndept = normalizeDeptName(dept)
			if ndept != '':
				deptList2.append(ndept)
		prr['dept'] = deptList2
			
		closeDateStr = entry['Closed Date'].strip()
		prr['closeDate'] = datetime.datetime.strptime(closeDateStr,CSVDTFormat2)  if closeDateStr != '' else None
		prr['status'] = entry['Status'].strip()
		prr['text'] = entry['Request Text'].strip()
		prr['closeReason'] = entry['Closure Reasons'].strip()
		prr['URL'] = entry['URL'].strip()
		prr['requestCo'] = entry['Requester Company'].strip()
		
		if prr['requestCo'] in ("","N/A","n/a","NA","None","none"):
			prr['requestCo'] = None
		else:
			reqCoTbl[ prr['requestCo'] ].append(prrID)
		
		statusTbl[ prr['status'] ] += 1
		if prr['closeDate'] != None:
			ncloseDate += 1
						
		prrTbl[prrID] = prr
		
	print('bldIndexTblCSV: NPRR=%d NMissDept=%d NMultDept=%d NCloseDate=%d' % \
		(len(prrTbl),nmissDept,nmultDept,ncloseDate))
	if startDate != None:
		print(f'bldIndexTblCSV: NOld={nolder} NRecent={nrecent}')
	
	return prrTbl
		
def loadDept_SD(inf):
	'''load SD's curated department list
	deptTbl: name -> {deptID,normName,desc,poc_id}
	'''
	
	reader = csv.DictReader(open(inf))
	deptTbl = {}
	for i,entry in enumerate(reader):
		# id,name,name2,description,poc_id
		name = entry['name']
		if name in deptTbl:
			print(f'loadDept_SD: dup name?!',{entry})
			continue
		
		deptTbl[ entry['name'] ] = {'normName': entry['name2'], 
									'deptID':   int(entry['id']),
									'desc':     entry['description'],
									'poc_id':   entry['poc_id']}	
	return deptTbl

def normalizeDeptName(dept):
	
	if dept not in DeptTbl_SD:
		print(f'normalizeDeptName: missing dept name?! {dept}')
		return dept
		
	ndept = DeptTbl_SD[dept]['normName']
	
	return ndept

def anlyzRedact(currDB,outdir):
	'''evaluate redaction: contrast CLOSED PRR w/ documents with/out "redaction" in title
	'''

	curs = currDB.cursor()
	cmd = 'select id,pretty_id,request_date,created_at,closed_date,closure_reasons,prr_state from prr'
	curs.execute(cmd)
	allPRR = curs.fetchall()

	print(f'anlyzRedact: NPRR={len(allPRR)}')

	deptTbl = {} # deptNormName -> year {info}
	
	nmissdept = 0
	nmissRedactPRR = 0
	allYears = [2018,2019,2020]
	
	allCloseReason = set()
	for prr in allPRR:
		(prrIdx,pretty_id,request_date,created_at,closed_date,closure_reasons,prr_state) = prr
		reqDate = datetime.datetime.strptime(request_date,NRDTformat)

		prrYear = reqDate.year

		cmd = 'select department_id from depreq where request_id=?'
		curs.execute(cmd,(prrIdx,))
		deptIdx = curs.fetchone()
		
		if deptIdx == None:
			nmissdept += 1
			continue
		
		deptIdx = deptIdx[0]

		cmd = 'select name from department where id=?'
		curs.execute(cmd,(deptIdx,))
		deptName = curs.fetchone()
	
		if deptName == None:
			print(f'anlyzDeptClearance: missing deptName deptIdx={deptIdx}?!')
			nmissdept += 1
			continue
		
		deptName = deptName[0]
		normDept = normalizeDeptName(deptName)

		if normDept not in deptTbl:
			deptTbl[normDept] = {}
			for y in allYears:
				deptTbl[normDept][y] = {'nprr': 1,'nclose':0, 'ndoc': [], 'fracRedact': [], 'closeDays': [], 'rcloseDays': []}
		else:
			deptTbl[normDept][prrYear]['nprr'] += 1

		if prr_state != 'Closed':
			continue
		
		deptTbl[normDept][prrYear]['nclose'] += 1

		if closure_reasons != None and closure_reasons.lower().find('redact') == -1:
			prrRedact = True
		else:
			prrRedact = False

		closeDate = datetime.datetime.strptime(closed_date,NRDTformat)
		closeDays = (closeDate - reqDate).days
		
		cmd = 'select id,title from document where request_id=?'
		curs.execute(cmd,(prrIdx,))
		docRows = curs.fetchall()
		ndoc = len(docRows)
		deptTbl[normDept][prrYear]['ndoc'].append(ndoc)
		
		nredact = 0
		for doc in docRows:
			(id,title) = doc
			if title.lower().find('redact') == -1:
				nredact += 1

		if nredact>0 and not prrRedact:
			nmissRedactPRR += 1
			
		fracRedact = float(nredact) / ndoc if ndoc>0 else 0.
		deptTbl[normDept][prrYear]['fracRedact'].append(fracRedact)
		
		if nredact>0: # or prrRedact:
			deptTbl[normDept][prrYear]['rcloseDays'].append(closeDays)
		else:
			deptTbl[normDept][prrYear]['closeDays'].append(closeDays)

	print(f'anlyzRedact: NMissRedactPRR={nmissRedactPRR}')
	
	allDept = sorted(list(deptTbl.keys()))
	allCR = sorted(list(allCloseReason))
	
	for year in allYears:
		outf = outdir + f'deptRedact_{year}.csv'
		outs = open(outf,'w')
			
		hdr = 'Dept,NPRR,NClose,AvgNDoc,AvgFracRedact,AvgCloseDay,AvgRedactDay'
		outs.write(hdr+'\n')
		for dept in allDept:
			info = deptTbl[dept][year]
			avgNDoc,sd = basicStats(info['ndoc'])
			avgFrac,sd = basicStats(info['fracRedact'])
			avgCloseDays,sd = basicStats(info['closeDays'])
			avgRedactDays,sd = basicStats(info['rcloseDays'])
			line = f'{dept},{info["nprr"]},{info["nclose"]},{avgNDoc},{avgFrac},{avgCloseDays},{avgRedactDays}'
			outs.write(line+'\n')
			
		outs.close()

def compdb2csv(currDB,prrCSVTbl,outf):
	'''210416: compare 210326 API database against 210322 CSV data
	'''

	curs = currDB.cursor()
	cmd = 'select id,pretty_id,request_date,created_at,closed_date,closure_reasons,prr_state from prr'
	curs.execute(cmd)
	allPRR = curs.fetchall()

	print(f'compdb2csv: NPRR={len(allPRR)}')

	deptTbl = defaultdict(lambda: defaultdict(lambda: defaultdict(int))) # deptNormName -> year -> ('db' | 'csv') -> freq
	
	nmissdept = 0
	allYears = [2018,2019,2020]
	
	fndCSV = set()
	nmissCSV = 0
	ndupCSV = 0
	for prrDB in allPRR:
		(prrIdx,pretty_id,request_date,created_at,closed_date,closure_reasons,prr_state) = prrDB
		reqDate = datetime.datetime.strptime(request_date,NRDTformat)

		prrYear = reqDate.year

		cmd = 'select department_id from depreq where request_id=?'
		curs.execute(cmd,(prrIdx,))
		
		# NB: fetching ALL department?!
		deptIdxList = curs.fetchall()
		
		if deptIdxList == None:
			nmissdept += 1
			continue
		
		deptNameList = []
		for didx in deptIdxList:
			deptIdx = didx[0]

			cmd = 'select name from department where id=?'
			curs.execute(cmd,(deptIdx,))
			deptName = curs.fetchone()
		
			if deptName == None:
				print(f'compdb2csv: missing deptName deptIdx={deptIdx}?!')
				nmissdept += 1
				continue
			
			deptName = deptName[0]
			normDept = normalizeDeptName(deptName)
			deptNameList.append(normDept)
	
			deptTbl[normDept][prrYear]['db'] += 1

		dbDeptSet = set(deptNameList)
			
		if pretty_id not in prrCSVTbl:
			print(f'compdb2csv: missing PRR in CSV? pretty_id={pretty_id}?!')
			nmissCSV += 1
			continue
		
		if pretty_id in fndCSV:
			print(f'compdb2csv: dupCSV?! pretty_id={pretty_id}?!')
			ndupCSV += 1
			
		fndCSV.add(pretty_id)
		prrCSV = prrCSVTbl[pretty_id]
		for csvDept in prrCSV['dept']:
			# NB: csv department names normalized in bldIndexTblCSV()
			deptTbl[csvDept][prrYear]['csv'] += 1
			
		csvDeptSet = set(prrCSV['dept'])
		
		if dbDeptSet != csvDeptSet:
			print(f'compdb2csv: different departments?! prrIdx={prrIdx} pretty_id={pretty_id}')
			dbDeptList = sorted(list(dbDeptSet))
			csvDeptList = sorted(list(csvDeptSet))
			print(f'\tDB:  {dbDeptList}')
			print(f'\tCSV: {csvDeptList}')
			print('')

	allCSVset = set(prrCSVTbl.keys())
	dbMissSet = allCSVset - fndCSV
		
	print(f'compdb2csv: NMissDept={nmissdept} NMissCSV={nmissCSV} ndupCSV={ndupCSV} NDBMiss={len(dbMissSet)}')
	
	allDept = sorted(list(deptTbl.keys()))
	outs = open(outf,'w')	
	hdr = 'Dept'
	for year in allYears:
		hdr += f',{year}_DB,{year}_CSV'
	outs.write(hdr+'\n')
	
	for dept in allDept:
		line = f'{dept}'
		for year in allYears:
			line += f',{deptTbl[dept][year]["db"]},{deptTbl[dept][year]["csv"]}'
		outs.write(line+'\n')	
	
	outs.close()
		
if __name__ == '__main__':
	
	dataDir = 'PATH-TO-DATAD/'

	# 210407: Restrict analysis to > Apr 1 2018
	startDate = datetime.datetime(2018,4,1,tzinfo=OaklandTimeZone)
	endDate =   datetime.datetime(2021,1,1,tzinfo=OaklandTimeZone)
	
	# 210415: use SD's updated table
	deptFile_SD = dataDir + 'sdoran-DeptLookup-emdash-v2.csv'
	global DeptTbl_SD
	DeptTbl_SD = loadDept_SD(deptFile_SD)

	## 210330: work from SD's downloaded json
	jsonDir = dataDir + 'API_data/'
	
	bldPRRdb(jsonDir,startDate,endDate)

	sys.exit()
	
	dbfile = dataDir +  'prr_210416.db'
	currDB = sqlite.connect(dbfile)

	# anlyzRedact(currDB,dataDir)
	
	## 210416: compare 210326 API data against 210322 CSV data
	
	csvFile = dataDir + 'requests-2021-03-22 redacted.csv'
	prrCSV = bldIndexTblCSV(csvFile,startDate,endDate)

	compFile = dataDir + 'db2csvComp.csv'
	compdb2csv(currDB,prrCSV,compFile)
	



