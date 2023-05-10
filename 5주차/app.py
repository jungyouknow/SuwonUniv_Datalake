import json
from bs4 import BeautifulSoup
import requests
from flask import Flask
import random

list_ = []
searchPath = 'http://www.yes24.com/Product/Search?domain=ALL&query=%ED%8C%8C%EC%9D%B4%EC%8D%AC&page='

for i in range(1,10):
   url = f'{searchPath}{i}'
   req = requests.get(url)
   html = req.text
   soup = BeautifulSoup(html, 'html.parser')
   totalBooks = soup.select('#yesSchList > li')
   
   for j in range(10):
      book = totalBooks[j]
      link = 'http://www.yes24.com' + book.select_one('a.gd_name')['href']
      title = book.select_one('a.gd_name').text
      author = book.find("span", {"class" : 'authPub info_auth'}).select_one('a').text
      publisher = book.find("span", {"class" : 'authPub info_pub'}).select_one('a').text
      pub_date = book.find("span", {"class" : 'authPub info_date'}).text
      price = book.find("strong", {"class" : 'txt_num'}).text
      dict_ = {"link": f"{link}","title": f"{title}", "author":f"{author}", "publisher":f"{publisher}","pub_date":f"{pub_date}","price":f"{price}"}
      json_data = json.dumps(dict_, ensure_ascii=False).encode('utf8')
      #value = json.loads(json_data)
      list_.append(json_data)


app = Flask(__name__)

@app.route("/")
def index():
   return 'Hellow World'

@app.route("/api")
def api():
   return list_[random.randint(0,len(list_)-1)]


if __name__ == "__main__":
   app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))