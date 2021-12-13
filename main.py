import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 320)

transfers_sites_list = [
    'https://www.transfermarkt.pl/pko-ekstraklasa/transfers/wettbewerb/PL1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/premier-league/transfers/wettbewerb/GB1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/laliga/transfers/wettbewerb/ES1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/bundesliga/transfers/wettbewerb/L1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/serie-a/transfers/wettbewerb/IT1/plus/?saison_id=2020&s_w=&leihe=3&intern=0&intern=1',
    'https://www.transfermarkt.pl/ligue-1/transfers/wettbewerb/FR1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/premier-liga/transfers/wettbewerb/RU1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/bundesliga/transfers/wettbewerb/A1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/campeonato-brasileiro-serie-a/transfers/wettbewerb/BRA1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/eredivisie/transfers/wettbewerb/NL1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/liga-portugal-bwin/transfers/wettbewerb/PO1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/superliga/transfers/wettbewerb/AR1N/plus/?saison_id=2019&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/major-league-soccer/transfers/wettbewerb/MLS1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1'
]

file_names = ["transfers-file-"+site[site.find('.pl')+4:site.find('/transfers')]+".html" for site in transfers_sites_list]
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'
}

for index, site in enumerate(transfers_sites_list):
    webpage = requests.get(site, headers=headers)
    file_name = file_names[index]
    file = open(file_name, "w+")
    file_names.append(file_name)
    file.write(webpage.text)
    file.close()

transfers_columns = ['Klub', 'KlubPoziom', 'KlubNarodowosc', 'Zawodnik', 'ZawodnikWiek', 'ZawodnikPozycja', 'ZawodnikPozycjaSkrot', 'ZawodnikNarodowosc', 'TypOperacji', 'Pozyskany/Sprzedany', 'ZawodnikWartoscRynkowa', 'KlubPowiazany', 'KlubPowiazanyPoziom', 'KlubPowiazanyNarodowosc', 'KwotaOdstepnego']
transfers = list()

club_league_info_dict = dict()


def get_club_info(club_href):
    club_info = club_league_info_dict.get(club_href, None)

    if club_info is None:
        club_page = requests.get("https://transfermarkt.pl"+club_href, headers=headers)
        club_page_soup = BeautifulSoup(club_page.text, 'html.parser')

        if len(club_page_soup.select('div.dataZusatzbox')) == 0:
            club_league_info_dict[club_href] = ["", ""]
            return ["", ""]

        club_league_level = club_page_soup.select("div.dataZusatzbox > div.dataZusatzDaten > span.mediumpunkt")[0].text.replace("\n", "").strip()[-6:]
        club_league_nationality = club_page_soup.select("div.dataZusatzbox > div.dataZusatzDaten > span.mediumpunkt > a > img")[0]["alt"].strip()
        club_league_info = [club_league_level, club_league_nationality]
        club_league_info_dict[club_href] = club_league_info
        club_info = club_league_info

    return club_info


def gather_info_from_file(file_name):
    file = open(file_name, "r")
    file_content = file.read()
    file.close()
    soup = BeautifulSoup(file_content, 'html.parser')
    transfers_tables = soup.select('#main > *:nth-child(13) > .large-8.columns > div')
    transfers_league = soup.select('#main .box-personeninfos > div.list:nth-child(1) > table.profilheader > tr:nth-child(1) > td')[0].text.strip().split("\xa0-\xa0\xa0")
    transfers_rows = list()
    for transfers_table_index in range(3, len(transfers_tables)):
        transfers_rows.append(gather_info_about_table(transfers_tables[transfers_table_index], transfers_league))

    return transfers_rows


def gather_info_about_table(table_div, league_info):
    club_name = table_div.select('.table-header > a')[0]['title']
    player_rows = list()
    for table in table_div.select('table'):
        bought_or_sold = table.select("thead > tr > th:nth-child(1)")[0].text
        for player in table.select('tbody > tr'):
            player_name = player.select('td:nth-child(1) > div > span > a')[0].text
            player_age = player.select('td:nth-child(2)')[0].text
            player_nationality = player.select('td:nth-child(3) > img')[0]['title']
            player_position = player.select('td:nth-child(4)')[0].text
            player_position_scut = player.select('td:nth-child(5)')[0].text
            player_value = player.select('td:nth-child(6)')[0].text
            player_club_assigned = player.select('td:nth-child(7) > a')[0]['title'] if len(player.select('td:nth-child(7) > a')) > 0 else ""
            player_club_assigned_info = get_club_info(player.select('td:nth-child(7) > a')[0]['href']) if len(player.select('td:nth-child(7) > a')) > 0 else ""
            player_club_assigned_level = player_club_assigned_info[0] if (len(player_club_assigned_info) == 2) else ""
            player_club_assigned_nationality = player_club_assigned_info[1] if (len(player_club_assigned_info) == 2) else ""
            player_compensation = player.select('td:nth-child(9) > a')[0].text.lower()
            player_transfer_or_loan = player.select('td:nth-child(9) > a')[0].text.lower()

            player_rows.append([club_name, league_info[0], league_info[1], player_name, player_age, player_position, player_position_scut, player_nationality, player_transfer_or_loan, bought_or_sold, player_value, player_club_assigned, player_club_assigned_level, player_club_assigned_nationality, player_compensation])

    return player_rows


def flatten_list(list_to_flat):
    return [e for sl in list_to_flat for e in sl]


for index, name in enumerate(file_names):
    transfers.append(gather_info_from_file(name))


transfers = flatten_list(flatten_list(transfers))
transfers_df = pd.DataFrame(transfers, columns=transfers_columns)
transfers_df.to_csv('csv/transfers-data-frame.csv', encoding='utf-8')