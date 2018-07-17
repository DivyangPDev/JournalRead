#!/usr/bin/env python
import sys, argparse, logging, json, copy, pprint, requests,time,operator
import boto3,json,decimal
import datetime
import dateutil.parser
import keys as k
from slackclient import SlackClient
from boto3.dynamodb.conditions import Key, Attr

pp = pprint.PrettyPrinter(indent=4)

def fetchJournalEntries(date):
	"""Main function to fetch and reconcile data from 4 different feeds"""
	
	pattern = '%d/%m/%Y'
	datetime_object = datetime.datetime.strptime(date, pattern)
	
	#Getting the feeds from respective feed functions
	slackFeed = getFromSlack(datetime_object)
	webServiceFeed = getFromWebService(datetime_object)
	githubFeed = getFromGitService(datetime_object)
	dynamoFeed = getFromDynamo(datetime_object)
	
	#Combining feeds into a single output
	entireFeed = reconcileFeed(slackFeed, webServiceFeed, githubFeed, dynamoFeed)
	
	return entireFeed

#Basic output template 
templateResult={'datetime': None, 'message': None, 'author': None, 'source': None}

def reconcileFeed(slackFeed, webServiceFeed, githubFeed, dynamoFeed):
	"""Reconciling all the feeds from different sources"""
	finalFeed = []
	if slackFeed is not None: finalFeed.extend(slackFeed)
	if webServiceFeed is not None: finalFeed.extend(webServiceFeed)
	if githubFeed is not None: finalFeed.extend(githubFeed)
	if dynamoFeed is not None: finalFeed.extend(dynamoFeed)

	#Calling the sort function to sort the finalFeed
	sortFinalFeed(finalFeed)

	return finalFeed

# Internal Functions
def validate(dateText):
	"""Validatng the date"""
	nowStr = datetime.datetime.now().strftime('%d/%m/%Y') 
	now = datetime.datetime.strptime(str(nowStr), "%d/%m/%Y")
	
	try:
		dateTextObj = datetime.datetime.strptime(dateText, "%d/%m/%Y")
	except ValueError:
		raise ValueError("Incorrect date format, should be DD/MM/YYYY")
	
	#Checking if the current date is greater than current date or not
	if dateTextObj > now:
		print("Date given cannot be greater than current date")


def sortFinalFeed(finalFeed): 
	"""Sorting finalFeed with datetime"""
	finalFeed.sort(key = lambda a: a['datetime'])
	return finalFeed	

def convertDateToTimeStamp(datetime_object):
	"""Converting datetime_object to timestamp"""
	pattern = '%Y-%m-%d %H:%M:%S'
	epoch = int(time.mktime(time.strptime(str(datetime_object), '%Y-%m-%d %H:%M:%S')))
	return epoch

def getFromDynamo(datetime_object):
	"""Get the journal entries from dynamoDB"""
	
	#Disabling info messages
	boto3.set_stream_logger('botocore.vendored.requests', logging.ERROR)
	
	#Converting date to dynamoDB pattern
	dynamoPattern = '%m/%d/%Y'
	dynamoDate = datetime_object.strftime(dynamoPattern)
	
	fetchedDynamoData = fetchDynamoFeed(dynamoDate)
	parsedData = parseDynamoFeed(fetchedDynamoData)
	return parsedData

def getFromSlack(datetime_object):
	"""Get the journal entries from slack channel"""
	
	#Converting the datetime_Object to startDateTime and endDateTime
	startDate = datetime.datetime(datetime_object.year, datetime_object.month, datetime_object.day,0,0,0)
	endDate = datetime.datetime(datetime_object.year, datetime_object.month, datetime_object.day,23,59,59)
	
	#Converting the startDate to startTimestamp and EndDate to Endtimestamp
	startTimestamp = convertDateToTimeStamp(startDate)
	endTimestamp = convertDateToTimeStamp(endDate)

	fetchedData = fetchSlackFeed(startTimestamp, endTimestamp)
	parsedData = parseSlackFeed(fetchedData)
	return parsedData

def getFromWebService(datetime_object):
	"""Get the journal entries from Web Endpoint"""

	#Converting date to Web endpoint pattern
	webEndPointPattern="%B %d %Y"
	webEndPointDate = datetime_object.strftime(webEndPointPattern)
	webEndPointDate = webEndPointDate.lstrip("0").replace(" 0", " ").strip()
	
	fetchedData = fetchWebFeed()
	parsedData = parseWebFeed(fetchedData,webEndPointDate)
	return parsedData	

def getFromGitService(datetime_object):
	"""Get the journal entries from Git channel"""
	startDate = datetime.datetime(datetime_object.year, datetime_object.month, datetime_object.day,0,0,0)
	endDate = datetime.datetime(datetime_object.year, datetime_object.month, datetime_object.day,23,59,59)

	fetchedGitHubData = fetchGithubFeed(startDate, endDate)
	parsedData = parseGithubFeed(fetchedGitHubData)
	return parsedData

def fetchDynamoFeed(datetime_object):
	"""Making the API call with Dynamo feed"""
	dynamodb = boto3.resource('dynamodb',  aws_access_key_id=k.DYNAMO_ACCESS_KEY,  aws_secret_access_key=k.DYNAMO_SECRET_ACCESS_KEY, region_name=k.DYNAMO_REGION_NAME)
	table  = dynamodb.Table(k.DYNAMO_TABLE)
	response = table.scan(
   		FilterExpression=Attr('date').eq(datetime_object) 
	)
	return response

def fetchSlackFeed(startTimestamp,endTimestamp):
	"""Making the API call with Slack feed"""
	sc = SlackClient(k.SLACK_TOKEN)
	response = sc.api_call(
		"conversations.history",
		channel = k.SLACK_CHANNEL_ID,
		oldest = startTimestamp,
		latest = endTimestamp,
		count = 100
	)
	return response	

def fetchGithubFeed(startDate,endDate):
	"""Making the API call with Github feed"""
	resp = requests.get(k.GITHUB_URL + "journal/commits?access_token=" + k.GITHUB_TOKEN + "&since=" + startDate.isoformat() + "&until=" + endDate.isoformat())
	
	return resp.json()

def fetchWebFeed():
	"""Making the API call with Web Endpoint feed"""
	resp = requests.get(url=k.URL)
	if resp.status_code != 200:
		raise ApiError('GET /tasks/ {}'.format(resp.status_code))
	return resp.json()

def parseDynamoFeed(data):
	"""parse the slack feed to standard format"""
	dynamoResult = []
	items = data['Items']
	if items:
		for item in items:
			date = item['date']
			date = dateutil.parser.parse(date).isoformat(' ').split('+')[0]
			date = datetime.datetime.strptime( date, "%Y-%m-%d %H:%M:%S" )
			name = item['name']
			text = item['text']
			itemResult = copy.deepcopy(templateResult)
			itemResult['message'] = text
			itemResult['author'] = name
			itemResult['datetime'] = date
			itemResult['source'] = 'DynamoDB'
			dynamoResult.append(itemResult)
	return dynamoResult

def parseSlackFeed(data):
	"""parse the slack feed to standard format"""
	slackResult = []
	messages = data.get('messages')
	if messages:	
		for message in messages:
			text = message.get('text')
			user = message.get('user')
			ts = message.get('ts')
			ts = ts.split('.')[0]
			ts = datetime.datetime.fromtimestamp(int(ts))
			subtype = message.get('subtype')
			item = copy.deepcopy(templateResult)
			item['message'] = text
			item['author'] = user
			item['datetime'] = ts
			item['source'] = 'slack'
			slackResult.append(item)
	return slackResult

def parseGithubFeed(data):
	"""parse the Github feed to standard format"""
	gitResult = []
	if data:
		for entries in data:
			text = entries['commit']['message']
			author = entries['commit']['author']['name']
			time = entries['commit']['author']['date']
			time = dateutil.parser.parse(time).isoformat(' ').split('+')[0] 
			time = datetime.datetime.strptime(time, "%Y-%m-%d %H:%M:%S" )
			item = copy.deepcopy(templateResult)
			item['message'] = text 
			item['author'] = author
			item['datetime'] = time
			item['source'] = 'Github'
			gitResult.append(item)
	return gitResult

def parseWebFeed(data,date_object):
	"""parse the Web Endpoint feed to standard format"""
	webResult = []
	if data:
		for repo in data['entries']:
			entryDate = repo['time'].split('at')[0]
			entryDate = entryDate.lstrip("0").replace(" 0", " ").strip()
			if entryDate == date_object:
				text = repo['text']
				sentiment = repo['sentiment']
				time = repo['time']
				time = dateutil.parser.parse(time).isoformat(' ').split('+')[0] 
				time = datetime.datetime.strptime( time, "%Y-%m-%d %H:%M:%S" )
				item = copy.deepcopy(templateResult)
				item['message'] = text + " , sentiment: " + sentiment
				item['datetime'] = time
				item['source'] = 'Web EndPoint'
				webResult.append(item)
	return webResult

def printJournal(entireFeed):
	"""Formatting the final Feed"""
	for feed in entireFeed:
		feed['datetime'] = str(feed['datetime'])
	pp.pprint(entireFeed)

def main(args, loglevel):
	"""Gather our code in a main() function"""
	logging.basicConfig(format="%(levelname)s: %(message)s", level=loglevel)
	logging.debug("Your Argument: %s" , args.date)
	validate(args.date)
	result = fetchJournalEntries(args.date)
	finalResult = printJournal(result)
	finalResult

if __name__ == '__main__':
	"""Standard boilerplate to call the main() function to begin the program"""
	parser = argparse.ArgumentParser( 
		description = "Specify the date you want to fetch the journal entries for in DD/MM/YYYY",
		epilog = "As an alternative to the commandline, params can be placed in a file, one per line, and specified on the commandline like '%(prog)s @params.conf'.",
		fromfile_prefix_chars = '@' )
	
	parser.add_argument(
		"date",
		help = "date",
		metavar = "ARG")	

	parser.add_argument(
		"-v",
		"--verbose",
		help="increase output verbosity",
		action="store_true")
	
	args = parser.parse_args()
	
	if args.verbose:
		loglevel = logging.DEBUG
	else:
		loglevel = logging.INFO
	
	main(args, loglevel)