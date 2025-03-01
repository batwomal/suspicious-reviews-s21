#!/usr/bin/env python3

from time import sleep

import httpx
import asyncio
import logging

import json
from getpass import getpass

logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(levelname)s - %(message)s',
	datefmt='%d-%b-%y %H:%M:%S',
	handlers=[
		logging.FileHandler('api.log'),
		logging.StreamHandler()
	]
)
logger = logging.getLogger()

class Api:

	def __init__(self):
		self.base_url = 'https://edu-api.21-school.ru/services/21-school/api'
		self.token = self.get_token()
		self.headers = {
			'Authorization': f'Bearer {self.token}',
			'Content-Type': 'application/json',
			'x-edu-org-unit-id': '6bfe3c56-0211-4fe1-9e59-51616caac4dd',
		}

	def get_token(self):
		try:
			with open ('token.json', 'r') as f:
				data = json.load(f)
				return data['access_token']
		except:
			headers = {
				'Content-Type': 'application/x-www-form-urlencoded',
			}

			data = f'client_id=s21-open-api&username={input("Login: ")}&password={getpass("Password: ")}&grant_type=password'

			response = httpx.post(
				'https://auth.sberclass.ru/auth/realms/EduPowerKeycloak/protocol/openid-connect/token',
				headers=headers,
				data=data,
			)
			with open ('token.json', 'w') as f:
				json.dump(response.json(), f, indent=4)
			return response.json()['access_token']

	async def request(
		self,
		method,
		url,
		headers=None,
		cookies=None,
		params=None,
		data=None,
		json=None,
	):
		status_code = None
		while not status_code:
			try:
				logger.info(f'Начало запроса к {url}')		
				async with httpx.AsyncClient() as client:
					response = await client.request(
						method=method,
						url=url,
						headers=headers,
						cookies=cookies,
						params=params,
						data=data,
						json=json,
					)
					response.raise_for_status()
					data = response.json()
					status_code = response.status_code
			except:
				if status_code != 429:
					status_code = None
					data = None	
		return {'status_code': status_code, 'json': data}

	def check_existing_file(self, file, func, params=None):
		try:
			with open(file, 'r') as f:
				data = json.load(f)
		except:
			data = func(params)
		return data

	async def get_campuses(self):
		tasks = [self.request('GET', self.base_url + '/v1/campuses', headers=self.headers)]
		response = await asyncio.gather(*tasks)
		with open('campuses.json', 'w') as f:
			json.dump(response[0]['json'], f, ensure_ascii=False, indent=4)
		return response[0]['json'] 

	async def get_coalitions(self):
		data = self.check_existing_file('campuses.json', self.get_campuses)
		campuses = data['campuses']

		tasks = [self.request('GET', self.base_url + f'/v1/campuses/{campus["id"]}/coalitions', headers=self.headers, params={'limit': 1000, 'offset': 0}) for campus in campuses]
		coalitions = await asyncio.gather(*tasks)
		coalitions = {campus['shortName']: response['json'] for campus, response in zip(campuses, coalitions)}

		# coalitions = [response['json'] for response in response]

		with open('coalitions.json', 'w') as f:
			json.dump(coalitions, f, ensure_ascii=False, indent=4)
		return coalitions

	def get_all_participants(self):
		try:
			with open('campuses.json', 'r') as f:
				campuses = json.load(f)
		except:
			campuses = self.get_campuses()

		campuses = campuses['campuses']
		campus_participants = {}
		for campus in campuses:
			offset = 0
			participants = []	
			while True:
				response = httpx.get(url=f'{self.base_url}/v1/campuses/{campus["id"]}/participants/', headers=self.headers, params={'limit': 1000, 'offset': offset})
				print(f'Campus: {campus["shortName"]} - {response.status_code}, offset: {offset}')
				if response.status_code == 200:
					data = response.json()['participants']
					if len(data) == 0:
						break
					participants.extend(data)
					offset += 1000
				
			campus_participants[campus['shortName']] = participants

		with open('all_participants.json', 'w') as f:
			json.dump(campus_participants, f, ensure_ascii=False, indent=4)

	def get_all_participants_with_coalitions(self):
		try:
			with open('coalitions.json', 'r') as f:
				coalitions = json.load(f)
		except:
			coalitions = self.get_coalitions()

		campus_participants = {}
		for campus in coalitions:
			campus_participants[campus] = {}
			for coalition in coalitions[campus]:
				campus_participants[campus][coalition['name']] = []
				offset = 0
			
				while True:
					response = httpx.get(url=f'{self.base_url}/v1/coalitions/{coalition["coalitionId"]}/participants/', headers=self.headers, params={'limit': 1000, 'offset': offset})
					print(f'Campus: {campus} - {coalition["name"]} - {response.status_code}, offset: {offset}')
					if response.status_code == 200:
						data = response.json()['participants']
						if len(data) == 0:
							break
						campus_participants[campus][coalition['name']].extend(data)
						offset += 1000

		with open('all_participants_with_coalitions.json', 'w') as f:
			json.dump(campus_participants, f, ensure_ascii=False, indent=4)

	def update_participants(self):
		def get_credentials_by_login(login):
			json_data = {
			'operationName': 'getCredentialsByLogin',
			'variables': {
					'login': login,
			},
			'query': 'query getCredentialsByLogin($login: String!) {\n  school21 {\n    getStudentByLogin(login: $login) {\n      studentId\n      userId\n      schoolId\n      isActive\n      isGraduate\n      __typename\n    }\n    __typename\n  }\n}\n',
			}
			status_code = None
			while status_code != 200:
				try:
					response = httpx.post('https://edu.21-school.ru/services/graphql', cookies=cookies, headers=headers, json=json_data)
					print(f'{login} - {response.status_code}')
					result = None
					if response.status_code == 200:
						data = response.json()['data']['school21']['getStudentByLogin']
						result = {
							'studentId': data['studentId'],
							'isActive': data['isActive'],
						}
					status_code = response.status_code
				except:
					status_code = None
			return result
		try:
			with open('all_participants_with_coalitions.json', 'r') as f:
				peers = json.load(f)
		except:
			peers = self.get_all_participants_with_coalitions() 

		logins = {}
		coalitions = ['Alpacas','Capybaras', 'Honeybagers', 'Salamanders']
		for coalition in coalitions:
			cnt = 1
			logins[coalition] = {}
			for peer in peers['21 Moscow'][coalition]:
				print(f'{cnt} - {coalition} {peer}')
				cnt += 1
				logins[coalition][peer] = get_credentials_by_login(peer)
		
		peers['21 Moscow'] = logins
		with open('peers.json', 'w') as f:
			json.dump(peers, f, ensure_ascii=False, indent=4)

	def get_project_info(self, login):

		with open(f'projects_{login}.json', 'r') as f:
			data = json.load(f)
			projectsID = [project['id'] for project in data['projects'] if (
				(project['status'] == 'ACCEPTED' or
				project['status'] == 'FAILED') and
				not 'EXAM' in project['type']
				)]

			query = '''

			query getProjectAttemptEvaluationsInfoByStudent($goalId: ID!, $studentId: UUID!) {
				school21 {
					getProjectAttemptEvaluationsInfo(goalId: $goalId, studentId: $studentId) {
						...ProjectAttemptEvaluations
						__typename
					}
					__typename
				}
			}

			fragment ProjectAttemptEvaluations on ProjectAttemptEvaluationsInfo {
				studentAnswerId
				attemptResult {
					...AtemptResult
					__typename
				}
				team {
					...AttemptTeamWithMembers
					__typename
				}
				p2p {
					...P2PEvaluation
					__typename
				}
				auto {
					status
					receivedPercentage
					endTimeCheck
					resultInfo
					__typename
				}
				codeReview {
					averageMark
					studentCodeReviews {
						user {
							avatarUrl
							login
							__typename
						}
						finalMark
						markTime
						reviewerCommentsCount
						__typename
					}
					__typename
				}
				__typename
			}

			fragment AtemptResult on StudentGoalAttempt {
				finalPointProject
				finalPercentageProject
				resultModuleCompletion
				resultDate
				__typename
			}

			fragment AttemptTeamWithMembers on TeamWithMembers {
				team {
					id
					name
					__typename
				}
				members {
					role
					user {
						...AttemptTeamMember
						__typename
					}
					__typename
				}
				__typename
			}

			fragment AttemptTeamMember on User {
				id
				avatarUrl
				login
				userExperience {
					level {
						id
						range {
							levelCode
							__typename
						}
						__typename
					}
					cookiesCount
					codeReviewPoints
					__typename
				}
				__typename
			}

			fragment P2PEvaluation on P2PEvaluationInfo {
				status
				checklist {
					...Checklist
					__typename
				}
				__typename
			}

			fragment Checklist on FilledChecklist {
				id
				checklistId
				endTimeCheck
				startTimeCheck
				reviewer {
					avatarUrl
					login
					businessAdminRoles {
						id
						school {
							id
							organizationType
							__typename
						}
						__typename
					}
					__typename
				}
				reviewFeedback {
					...EvaluationFeedback
					__typename
				}
				comment
				receivedPoint
				receivedPercentage
				quickAction
				checkType
				onlineReview {
					...OnlineReviewInfo
					__typename
				}
				__typename
			}

			fragment EvaluationFeedback on ReviewFeedback {
				id
				comment
				filledChecklist {
					id
					__typename
				}
				reviewFeedbackCategoryValues {
					feedbackCategory
					feedbackValue
					id
					__typename
				}
				__typename
			}

			fragment OnlineReviewInfo on OnlineReview {
				isOnline
				videos {
					onlineVideoId
					link
					status
					statusDetails
					updateDateTime
					fileSize
					__typename
				}
				__typename
			}
			'''

		projects = {}

		for project_id in projectsID:
			json_data = {
					'operationName': 'getProjectAttemptEvaluationsInfoByStudent',
					'variables': {
							'goalId': project_id,
							'studentId': 'a7cc13eb-1c0d-4714-9475-cb793b316b66',
					},
					'query': query
			}


			response = httpx.post('https://edu.21-school.ru/services/graphql', json=json_data, headers=self.headers)
			print(response.status_code)
			data = json.loads(response.text)['data']['school21']['getProjectAttemptEvaluationsInfo']
			for eval in data:
				eval['auto']['resultInfo'] = None 

			projects[project_id] = data
		
		with open(f'participant_{json_data["variables"]["studentId"]}.json', 'w', encoding='utf-8') as f:
			json.dump(projects,f, ensure_ascii=False, indent=4)

	def get_projects(self, login):
		response = httpx.get(url=f'{self.base_url}/v1/participants/{login}/projects', headers=self.headers, params={'limit': 1000, 'offset': 0})
		with open(f'projects_{login}.json', 'w', encoding='utf-8') as f:
			json.dump(response.json(), f, ensure_ascii=False, indent=4)
		return response.json()

	def get_coins(self):
		try:
			with open('all_participants_with_coalitions.json', 'r') as f:
				peers = json.load(f)
		except:
			peers = self.get_all_participants_with_coalitions() 

		coalitions = ['Alpacas','Capybaras', 'Honeybagers', 'Salamanders']

		coalitions = [coalition for coalition in peers['21 Moscow'] if coalition not in coalitions ]

		for coalition in coalitions:
			coins = {}
			cnt = 1
			for peer in peers['21 Moscow'][coalition]:
				status_code = None
				while status_code != 200:
					try:
						response = httpx.get(url=f'{self.base_url}/v1/participants/{peer}/points', headers=self.headers)
						print(f'{cnt} - Coaltion: {coalition}, Peer: {peer} - {response.status_code}')
						status_code = response.status_code
					except:
						status_code = None
						pass
				coins[peer] = response.json()['coins']
				cnt += 1

			coins = dict(sorted(coins.items(), key=lambda x: x[1], reverse=True))

			with open(f'coins_{coalition.lower()}.json', 'w') as f:
				json.dump(coins, f, ensure_ascii=False, indent=4)

		return coins

if __name__ == '__main__':
	api = Api()
	# api.basic_participant_info()
	# api.get_all_participants()
	# asyncio.run(api.get_campuses())
	asyncio.run(api.get_coalitions())
	# api.get_all_participants_with_coalitions()
	# api.update_participants()
	# api.get_coins()
	# api.get_projects('sherypan')
	# api.get_project_info('sherypan')
	