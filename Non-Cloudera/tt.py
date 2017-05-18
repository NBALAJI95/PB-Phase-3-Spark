import re
import json
import goslate

gs = goslate.Goslate()

f = open('INPUT_FILE.json', encoding="utf-8")
new_f = open('INPUT.txt','a')
#new_f.write('User_Screen_Name,Verified,Follower_Count,Language,Tweets_Count\n')
i = 0
while True:
	a = f.readline()
	if len(a) < 1:
		break
	else:
		try:
			#write_value = str()
			parsed_json = json.loads(a)
			tz = parsed_json['user']['time_zone']
			matches = re.findall(r'#\w*', parsed_json['text'])
			for j in matches:
				if tz!=None:
					new_f.write(str(j)[1:].lower()+','+str(tz)+'\n')	
			#write_value = parsed_json['user']['screen_name'].replace(',',';') + ',' + str(parsed_json['user']['verified']) + ',' + str(parsed_json['user']['followers_count']) + ',' + str(parsed_json['user']['lang']) + ','+ str(parsed_json['user']['statuses_count'])+ '\n'
			#new_f.write(write_value)
			#write_value = ''
		except:
			print('error',i)
			i+=1