import requests
from bs4 import BeautifulSoup
import csv

##############################上市##################################
url = "https://isin.twse.com.tw/isin/class_main.jsp"
res = requests.get(url, params={
    "market": "1",
    "issuetype": "1",
    "Page": "1",
    "chklike": "Y"
})

res.encoding = "MS950"
res_html = res.text

soup = BeautifulSoup(res_html, "lxml")

tr_list = soup.find_all("table")[1].find_all("tr")
tr_list.pop(0)


sid=[]
sname=[]
stype=[]
sclass=[]
sid.append('代號')
sname.append('名稱')
stype.append('櫃別')
sclass.append('類股')
for tr in tr_list:
    td_list=tr.find_all("td")
    sid.append(td_list[2].text)
    sname.append(td_list[3].text)
    stype.append('上市')
    sclass.append(td_list[6].text)


##############################上櫃##################################
url = "https://isin.twse.com.tw/isin/class_main.jsp"
res = requests.get(url, params={
    "market": "2",
    "issuetype": "4",
    "Page": "1",
    "chklike": "Y"
})

res.encoding = "MS950"
res_html = res.text

soup = BeautifulSoup(res_html, "lxml")

tr_list = soup.find_all("table")[1].find_all("tr")
tr_list.pop(0)

for tr in tr_list:
    td_list=tr.find_all("td")
    sid.append(td_list[2].text)
    sname.append(td_list[3].text)
    stype.append('上櫃')
    sclass.append(td_list[6].text)





with open('stock_id_all.csv','w',newline='',encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    for i in range(0,len(sid)):
        writer.writerow([sid[i],sname[i],stype[i],sclass[i]])