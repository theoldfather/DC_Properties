import sys
import os
import mechanize
from bs4 import BeautifulSoup as soup
import json
import pandas as pd
import re


def clean_str(s):
    txt =  s.replace("\n"," ").replace("\t","").strip().encode("utf-8","ignore")
    txt = txt.replace("\r"," ")
    txt = re.sub(r'\xc2|\xa0',' ',txt)
    return txt

def reduce_spaces(s):
    return re.sub("[ ]{2,}"," ",s)

def saveWardSales(ward=1,page=1,verbose=False):
    last_page=999
    page_dfs=[]
    header = ["ssl","premise_address","owner_name","neighborhood","sub_neighborhood","use_code",
              "sale_price","recordation_date","total_assessment"]
    br = mechanize.Browser()    
    while page <= last_page:
        if verbose and page % 10==0: print "processing page %d" % page
        if page==1:
            br.open("https://www.taxpayerservicecenter.com/RP_Results.jsp?search_type=Sales")
            br.select_form(name="SearchForm")
            br["selectWard"]=[str(ward)]
            resp = br.submit()
            html = soup(resp.read(),"html.parser")

            # set the number of pages to crawl for this ward
            href=html.find("a",text="last").attrs['href']
            m=re.search(r"page=([0-9]*)",href)
            last_page = int(m.group(1))
        else:
            resp=br.open("https://www.taxpayerservicecenter.com/RP_Results.jsp?page=%d"%page)
            html = soup(resp.read(),"html.parser")    

        table = html.find("table",attrs={"class":"","border":"0","cellpadding":4,"cellspacing":2})
        i = 0
        rows=list()
        for tr in table.find_all("tr"):  
            if i>0:
                row = [clean_str(td.text) for td in tr.find_all("td")]
                values = row
                row = { e[0]:e[1] for e in zip(header,values) }                
                rows.append(row)
            i+=1
        page_dfs.append(pd.DataFrame(rows,dtype=str))
        page += 1

    ward_df = pd.concat(page_dfs,ignore_index=True)
    ward_df.to_csv("../data/ward%d.csv" % (ward),index=False)
    return ward_df

def getDetails(ssl,br=None):
    if br is None:
        br = mechanize.Browser() 
        br.open("https://www.taxpayerservicecenter.com/RP_Results.jsp?search_type=Sales")
    resp = br.open("https://www.taxpayerservicecenter.com/RP_Detail.jsp?ssl=%s" % (ssl.replace(" ",'%20')))
    html= soup(resp.read(),"html.parser")
    tables = html.find_all("table",attrs={"cellspacing":"2"})
    items = {"ssl":ssl}
    if tables>0:
        for table in tables[:3]:    
            for tr in table.find_all("tr"):
                capture_next=False
                for td in tr.find_all("td"):
                    if not capture_next:
                        capture_next = not (re.search(":",td.text) is None)
                        label=td.text.replace(":","")
                    else:
                        value = reduce_spaces(clean_str(td.text))
                        value = value.replace("; "," ")
                        items.update({label:value})
                        capture_next=False
    return items

def getFeatures(ssl,br=None):
    if br is None:
        br = mechanize.Browser() 
        br.open("https://www.taxpayerservicecenter.com/RP_Results.jsp?search_type=Sales")
    resp = br.open("https://www.taxpayerservicecenter.com/weblogic/CAMA?ssl=%s" % (ssl.replace(" ",'%20')))
    html= soup(resp.read(),"html.parser")
    tables = html.find_all("table",attrs={"cellspacing":"2","cellpadding":"2","align":"left"})
    items = {"ssl":ssl}
    if tables>0:
        for table in tables[:3]:    
            for tr in table.find_all("tr"):
                capture_next=False
                for td in tr.find_all("td"):
                    if not capture_next:                        
                        label=td.text
                        capture_next=True
                    else:
                        value = reduce_spaces(clean_str(td.text))
                        value = value.replace("; "," ")
                        items.update({label:value})
                        capture_next=False
    return items

def mapSave(f,ssls,filename,chunk_size=100,resume=True):
    dfs=list()
    if os.path.exists(filename) and resume:
        prev = pd.read_csv(filename)
        dfs.append(prev)
        ssls = list(set(ssls).difference(set(prev['ssl'])))
    i=0
    chunk = ssls[i*chunk_size:(i+1)*chunk_size]
    while len(chunk)>0:
        # refresh browser
        br = mechanize.Browser() 
        br.open("https://www.taxpayerservicecenter.com/RP_Results.jsp?search_type=Sales")
        # scrape
        dfs.append(pd.DataFrame([f(c,br) for c in chunk]))
        pd.concat(dfs,ignore_index=True).to_csv(filename,index=False)
        i += 1
        chunk = ssls[i*chunk_size:(i+1)*chunk_size]

class Status(object):
    
    def __init__(self,iterable):
        self.n = float(len(iterable))
        self.i = 0
        
    def pprint(self,every=100):
        if self.i % every == 0: print "completed %02.4f%%" % (self.i/self.n)
        self.i += 1
        return True


if __name__ == "__main__":

    if sys.argv[1]=="features":
        print "getting features.."
        all_wards = pd.read_csv("../data/sales.csv")
        ssls=all_wards['ssl']
        mapSave(getFeatures,ssls,"../data/features.csv") 
    elif sys.argv[1]=="details":
        # details
        print "getting details.."
        all_wards = pd.read_csv("../data/sales.csv")
        ssls=all_wards['ssl']
        mapSave(getDetails,ssls,"../data/details.csv") 

