import pandas as pd
import csv

# 代碼,名稱,櫃別,類股
data = pd.read_csv("C:/Users/Administrator/Documents/TSE-Stock-Crawler/stock_id_all.csv")
stock_ids = [ str(i) for i in data['代碼'].to_list() ]
stock_name = [ str(i) for i in data['名稱'].to_list() ]
stock_type = [ str(i) for i in data['櫃別'].to_list() ]
stock_class = [ str(i) for i in data['類股'].to_list() ]

sid=[]
sname=[]
stype=[]
sclass=[]

for i in range(0,len(stock_class)):
    if(stock_class[i]=="指數"):
        sid.append(stock_ids[i])
        sname.append(stock_name[i])
        stype.append(stock_type[i])
        sclass.append(stock_class[i])
    elif(stock_class[i]=="金融保險業"):
        sid.append(stock_ids[i])
        sname.append(stock_name[i])
        stype.append(stock_type[i])
        sclass.append(stock_class[i])
    elif(stock_class[i]=="電腦及週邊設備業"):
        sid.append(stock_ids[i])
        sname.append(stock_name[i])
        stype.append(stock_type[i])
        sclass.append(stock_class[i])
    elif(stock_class[i]=="電子零組件業"):
        sid.append(stock_ids[i])
        sname.append(stock_name[i])
        stype.append(stock_type[i])
        sclass.append(stock_class[i])
    elif(stock_class[i]=="電機機械"):
        sid.append(stock_ids[i])
        sname.append(stock_name[i])
        stype.append(stock_type[i])
        sclass.append(stock_class[i])
    elif(stock_class[i]=="其他電子業"):
        sid.append(stock_ids[i])
        sname.append(stock_name[i])
        stype.append(stock_type[i])
        sclass.append(stock_class[i])
    elif(stock_class[i]=="電子通路業"):
        sid.append(stock_ids[i])
        sname.append(stock_name[i])
        stype.append(stock_type[i])
        sclass.append(stock_class[i])
    elif(stock_class[i]=="半導體業"):
        sid.append(stock_ids[i])
        sname.append(stock_name[i])
        stype.append(stock_type[i])
        sclass.append(stock_class[i])



#上市
tse_sid=[]
tse_sname=[]
tse_stype=[]
tse_sclass=[]

for i in range(0,len(sid)):
    if(stype[i]=="上市"):
        tse_sid.append(sid[i])
        tse_sname.append(sname[i])
        tse_stype.append(stype[i])
        tse_sclass.append(sclass[i])

with open('TSE_Important_StockID.csv','w',newline='',encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    for i in range(0,len(tse_sid)):
        writer.writerow([tse_sid[i],tse_sname[i],tse_stype[i],tse_sclass[i]])

#上櫃
otc_sid=[]
otc_sname=[]
otc_stype=[]
otc_sclass=[]

for i in range(0,len(sid)):
    if(stype[i]=="上櫃"):
        otc_sid.append(sid[i])
        otc_sname.append(sname[i])
        otc_stype.append(stype[i])
        otc_sclass.append(sclass[i])

with open('OTC_Important_StockID.csv','w',newline='',encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile, delimiter=',')
    for i in range(0,len(otc_sid)):
        writer.writerow([otc_sid[i],otc_sname[i],otc_stype[i],otc_sclass[i]])