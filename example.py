from urlparse import urlparse, parse_qsl

url = 'http://localhost:8000/?userID=42839&operator=0&count=1&resourceID="323,456,3245"&key=name'
#print parse_qsl(urlparse(url)[4])

a = ['Alice', 'Ben', 'Ren']
for item in a:
	print item