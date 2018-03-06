from urllib2 import urlopen
from bs4 import BeautifulSoup

url = "http://www.imdb.com/search/title?sort=num_votes,desc&start=1&title_type=feature&year=2005,2014"
source = urlopen(url).read()

data = BeautifulSoup(source, 'lxml')

lists = data.find_all('div', {'class': 'lister-item mode-advanced'})

for i in lists:
    print i.find('h3').get_text(' ', strip=True)
    print "\t Genre: ", i.find('span', {'class': 'genre'}).get_text(' ', strip=True)
    print "\t Runtime: ", i.find('span', {'class': 'runtime'}).get_text(' ', strip=True)
    print "\t Rating: ", i.find('div', {'class': 'inline-block ratings-imdb-rating'}).get_text(' ', strip=True)
    print "\t Gross: ", i.find('span', {'class': 'text-muted'}).get_text(' ', strip=True)
    #print "\t Gross: ", i.find('div', {'class': 'sort-num_votes-visible'}).find('span',{'class': 'text-muted'}).get_text(' ', strip=True)