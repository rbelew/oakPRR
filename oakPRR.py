'''oakPRR: analysis of Oakland's Public Record Requests
Created on Mar 25, 2021

@author: rik, sdoran
'''

import re

DeptNorm = {'ADMIN_BUILDING_INSPECTION': 'PLANNING_BUILDING',
			'ADMIN_PLANNING_BUILDING_NEIGHBORHOOD_PRESERV': 'PLANNING_BUILDING',
			'BUDGET_AND_FISCAL': 'BUDGET_AND_REVENUE_REVENUE_DIVISION',
			'CITY_ATTORNEY_ADMINISTRATION_UNIT': 'CITY_ATTORNEY',
			'CITY_AUDITOR_UNIT': 'CITY_AUDITOR',
			'CITY_CLERK_UNIT': 'CITY_CLERK',
			'CONTRACTS_AND_COMPLIANCE': 'CONTRACTS_COMPLIANCE',
			'FINANCE_DEPT_REVENUE_BUSINESS_TAX': 'FINANCE_DEPARTMENT_CONTROLLER_BUDGET',
			'FIRE': 'FIRE_DEPARTMENT',
			'HUMAN_RESOURCES_MANAGEMENT': 'HUMAN_RESOURCES',
			'INFORMATION_TECHNOLOGY_IT_': 'INFORMATION_TECHNOLOGY',
			'OAKLAND_POLICE_DEPARTMENT': 'POLICE_DEPARTMENT',
			'PUBLIC_WORKS_AGENCY': 'PUBLIC_WORKS',
			'TRANSPORTATION_SERVICES_ADMINISTRATION': 'DEPARTMENT_OF_TRANSPORTATION'}

PRRNoise = set(['oakland', 'document', 'request', 'record','report','pleas','provid',
				'public','inform','copi','would','citi','california','ca','includ'])

CSVDTFormat = '%m/%d/%y %H:%M'

def normDeptName(dept):
	'''convert all department names to upper case, space(s) replaced with '_'
	'''
	ndept1 = re.sub('\W','_',dept.upper())
	ndept2 = ndept1.replace('___','_')
	ndept3 = ndept2.replace('__','_')
	return ndept3

def openStatusP(s):
	return s in [ 'Open','Overdue','Due soon']

if __name__ == '__main__':
	pass