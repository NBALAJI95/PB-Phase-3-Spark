f = open('fetched_tweets_output.json')
ff = open('INPUT_FILE.json','a')
while True:
	a = f.readline()
	print(a)
	if a == '\n' or len(a)<60:
		continue
	else:
		ff.write(a)
f.close()
ff.close()