import json

f = open('INPUT_FILE.json')
new_f = open('INPUT.csv','a')
new_f.write('User_Screen_Name,Verified,Follower_Count,Language,Tweets_Count\n')
i = 0
while True:
	a = f.readline()
	if len(a) < 1:
		break
	else:
		try:
			write_value = str()
			parsed_json = json.loads(a)
			write_value = parsed_json['user']['screen_name'].replace(',',';') + ',' + str(parsed_json['user']['verified']) + ',' + str(parsed_json['user']['followers_count']) + ',' + str(parsed_json['user']['lang']) + ','+ str(parsed_json['user']['statuses_count'])+ '\n'
			new_f.write(write_value)
			write_value = ''
		except:
			i+=1
			print('Error', i)