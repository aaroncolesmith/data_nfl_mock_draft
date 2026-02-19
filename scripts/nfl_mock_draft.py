import pandas as pd
import requests
import datetime
import numpy as np
import os
from bs4 import BeautifulSoup
import time

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


import json

year='2026'

def get_react_props(url):
    try:
        req = requests.get(url, headers=headers, timeout=15)
        if req.status_code != 200:
            print(f"Error fetching {url}: {req.status_code}")
            return None
        soup = BeautifulSoup(req.content, 'html.parser')
        # Check both Index and Show components
        for component_name in ["mocks/Index", "mocks/Show"]:
            div = soup.find("div", {"data-react-class": component_name})
            if div and "data-react-props" in div.attrs:
                return json.loads(div["data-react-props"])
    except Exception as e:
        print(f"Exception fetching {url}: {e}")
    return None

def mock_to_df(mock_info):
    url = 'https://www.nflmockdraftdatabase.com' + mock_info['url']
    print(url)
    
    props = get_react_props(url)
    if not props or "mock" not in props:
        return pd.DataFrame()

    mock_data = props["mock"]
    selections = mock_data.get("selections", [])
    
    picks = []
    teams = []
    team_imgs = []
    players = []
    player_details = []

    for sel in selections:
        player = sel.get("player", {})
        team = sel.get("team", {})
        college = player.get("college", {})
        
        picks.append(sel.get("pick"))
        
        # Extract team name from URL if possible
        team_url = team.get("url", "")
        team_name = team_url.split("/")[-1].replace("-", " ").title() if team_url else "Unknown"
        teams.append(team_name)
        
        logo_url = team.get("logo", "")
        if logo_url.startswith("/"):
            logo_url = "https://www.nflmockdraftdatabase.com" + logo_url
        team_imgs.append(logo_url)
        players.append(player.get("name", "Unknown"))
        
        details = f"{player.get('position', '')}, {college.get('name', '')}".strip(", ")
        player_details.append(details)

    d = pd.DataFrame({
        'pick': picks,
        'team': teams,
        'team_img': team_imgs,
        'player': players,
        'player_details': player_details
    })
    
    d['source'] = mock_data.get("name", "Unknown")
    d['url_path'] = mock_info['url']
    d['date'] = mock_data.get("published_at", "")[:10] # YYYY-MM-DD

    return d

df = pd.DataFrame()
i = 0
for u in range(1, 4):
    # url = f'https://www.nflmockdraftdatabase.com/mock-drafts/{year}/page/{u}'
    url = f'https://www.nflmockdraftdatabase.com/mock-drafts/{year}?page={u}'
    print(url)
    props = get_react_props(url)
    
    if not props or "mocks" not in props:
        print(f"No mocks found on page {u}")
        continue
    
    mocks = props["mocks"]
    print(len(mocks))

    for mock_info in mocks:
        try:
            d = mock_to_df(mock_info)
            if not d.empty:
                df = pd.concat([df, d], ignore_index=True)
            else:
                i += 1
        except Exception as e:
            print(f"Error processing mock: {e}")
            i += 1


with open('last_updated.txt', 'a') as fp:
    fp.write(str(datetime.datetime.now())[:19])

df = df.drop_duplicates(subset=['pick','team','url_path'])
df = df.loc[df['pick']<=32].reset_index(drop=True)
df.to_csv(f'./data/new_nfl_mock_draft_db_{year}.csv',index=False)



df['pick']=pd.to_numeric(df['pick'])

df['team_pick'] = 'Pick '+ df['pick'].astype('str').replace('\.0', '', regex=True) + ' - ' +df['team']
df=df.loc[~df.team_pick.str.contains('/Colleges')]



print(f'had a total of {str(i)} failures')
