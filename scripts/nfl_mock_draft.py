import pandas as pd
import requests
import datetime
import numpy as np
import os
from bs4 import BeautifulSoup

headers = {
    'Host': 'www.nflmockdraftdatabase.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Content-Type': 'text/html; charset=UTF-8',
    'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-User': '?1',
    'Sec-Fetch-Dest': 'document',
    'Referer': 'https://www.nflmockdraftdatabase.com/mock-drafts/2021/nfl-mocks-2021-a-j-fagerlin',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cookie': 'announcement_seen=1; announcement_seen=1; _nflmockdb_session=ZkJIWFdiMnllVkM0SGNMeHBxUE56VlVRa3lCR0N6ampwUzNLYnRxSy9JU256bENQb2k0cXZqallIWjNaWElPR2VQcVVXUmVOY0pGcS84MG81WFNJd2pnRHJJWVZVenRzdEg5SERjR0tEU0RELzRBMzFLL1Zsa3dYMld4Z2diWEpPK2pHZlVpbmplZkdGNFJNdlpDY0RnPT0tLWNjZDgyUGhLeGEyRG9xS3dndUdzZXc9PQ%3D%3D--f84bf880c0f79c2eccf440638588c5aaf682859e',
    'If-None-Match': 'W/"fe2fe61b64b30d8eff802d92afd8da1b"'

}


year='2025'

def link_to_df(link):
    # try:
    url = 'https://www.nflmockdraftdatabase.com'+link['href']
    print(url)

    req = requests.get(url,headers=headers)
    soup = BeautifulSoup(req.content, 'html.parser')

    pick=[]
    team=[]
    team_img=[]
    player=[]
    player_details=[]

    table = soup.find('ul', class_="mock-list")

    for li in table.find_all("li"):
        pick.append(li.find('div',{"class":"pick-number"}).text)
        team.append(str(li).split(f'href="/teams/{year}/')[1].split('">')[0].replace('-',' ').title())
        team_img.append(str(li).split('loading="lazy" src="')[1].split('"/>')[0])
        try:
            player.append(li.find('div',{"class":"player-name player-name-bold"}).text)
        except:
            player.append(li.find('div',{"class":"player-name strikethrough"}).text)
        player_details.append(li.find('div',{"class":"player-details"}).text)

    d = pd.DataFrame(
                    {
                        'pick':pick,
                        'team':team,
                        'team_img':team_img,
                        'player':player,
                        'player_details':player_details
                    })
    d['source']=soup.find('h1').text
    d['date'] = url[-10:]

    return d


df=pd.DataFrame()
i=0
for u in range(1,4):

  url = f'https://www.nflmockdraftdatabase.com/mock-drafts/{year}/page/'+str(u)
  req = requests.get(url,headers=headers)
  soup = BeautifulSoup(req.content, 'html.parser')
  links = soup.find_all("a", class_="site-link")
  print(url)
  print(len(links))
  if len(links) == 0:
    time.sleep(10)
    req = requests.get(url,headers=headers)
    soup = BeautifulSoup(req.content, 'html.parser')
    links = soup.find_all("a", class_="site-link")
    print(f'second time -- {url}')
    print(len(links))


  for link in links:
    try:
      d = link_to_df(link)
      df=pd.concat([df,d])
    except:
      print('had a first failure ' + str(req.status_code))
      time.sleep(20)
      try:
        d = link_to_df(link)
        df=pd.concat([df,d])
      except:
        print('had a second failure ' + str(req.status_code))
        time.sleep(15)
        try:
          d = link_to_df(link)
          df=pd.concat([df,d])
        except:
          print('had a third failure :(' + str(req.status_code))
          i+=1
          print(f'total failur count: ({str(i)})')


with open('last_updated.txt', 'a') as fp:
    fp.write(str(datetime.datetime.now())[:19])
df.to_csv(f'./data/new_nfl_mock_draft_db_{year}.csv',index=False)



df['pick']=pd.to_numeric(df['pick'])

df['team_pick'] = 'Pick '+ df['pick'].astype('str').replace('\.0', '', regex=True) + ' - ' +df['team']
df=df.loc[~df.team_pick.str.contains('/Colleges')]

print(f'had a total of {str(i)} failures')
