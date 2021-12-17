import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import progressbar as pb

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
    'https://www.transfermarkt.pl/scottish-premiership/transfers/wettbewerb/SC1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/super-liga-srbije/transfers/wettbewerb/SER1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/premier-liha/transfers/wettbewerb/UKR1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/jupiler-pro-league/transfers/wettbewerb/BE1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/super-league/transfers/wettbewerb/C1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/super-lig/transfers/wettbewerb/TR1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/chinese-super-league/transfers/wettbewerb/CSL/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/qatar-stars-league/transfers/wettbewerb/QSL/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/super-league-1/transfers/wettbewerb/GR1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/1-hnl/transfers/wettbewerb/KR1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/superligaen/transfers/wettbewerb/DK1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/fortuna-liga/transfers/wettbewerb/TS1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/protathlima-cyta/transfers/wettbewerb/ZYP1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/eliteserien/transfers/wettbewerb/NO1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/ligat-haal/transfers/wettbewerb/ISR1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/allsvenskan/transfers/wettbewerb/SE1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/efbet-liga/transfers/wettbewerb/BU1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/championship/transfers/wettbewerb/GB2/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/league-one/transfers/wettbewerb/GB3/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/nemzeti-bajnoksag/transfers/wettbewerb/UNG1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/nemzeti-bajnoksag/transfers/wettbewerb/FI1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/liga-mx-clausura/transfers/wettbewerb/MEX1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/saudi-professional-league/transfers/wettbewerb/SA1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/j1-league/transfers/wettbewerb/JAP1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/uae-pro-league/transfers/wettbewerb/UAE1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/liga-dimayor-ii/transfers/wettbewerb/COL1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1',
    'https://www.transfermarkt.pl/egyptian-premier-league/transfers/wettbewerb/EGY1/plus/?saison_id=2020&s_w=&leihe=1&intern=0&intern=1'
]

file_names = ["html/transfers-file-"+site[site.find('.pl')+4:site.find('/transfers')]+site[site.find('werb')+5:site.find('/plus')]+".html" for site in transfers_sites_list]
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'
}

for index, site in enumerate(transfers_sites_list):
    webpage = requests.get(site, headers=headers)
    file_name = file_names[index]
    file = open(file_name, "w+", encoding="utf-8")
    file_names.append(file_name)
    file.write(webpage.text)
    file.close()

transfers_columns = ['Club', 'ClubLeagueLevel', 'ClubLeagueNationality', 'Player', 'PlayerAge', 'PlayerHeight',
                     'TransferDate', 'PlayerMatchesPlayed', 'PlayerGoals', 'PlayerAssists', 'PlayerOwnGoals',
                     'PlayerSubstitutionIn', 'PlayerSubstitutionOut', 'PlayerYellowCards', 'PlayerSecondYellowCards',
                     'PlayerRedCards', 'PlayerMinutesPlayed', 'PlayerPosition', 'PlayerPositionScut',
                     'PlayerNationality', 'Operation', 'Bought/Sold', 'PlayerMarketValue', 'ClubAssigned',
                     'ClubAssignedLeagueLevel', 'ClubAssignedLeagueNationality', 'TransferValue']
transfers = list()

club_league_info_dict = dict()
player_info_dict = dict()


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


def get_player_additional_info(player_href, player_position):
    player_info = player_info_dict.get(player_href, None)

    if player_info is None:
        player_fetched_info = list()
        transfermarkt_prefix = "https://www.transfermarkt.pl"
        player_name = player_href.split("/")[1]
        player_id = player_href.split("/")[-1]

        full_href = transfermarkt_prefix + "/" + player_name + "/leistungsdaten/spieler/" + player_id + "/plus/1?saison=ges"

        player_page = requests.get(full_href, headers=headers)
        player_page_soup = BeautifulSoup(player_page.text, 'html.parser')

        player_height_soup = player_page_soup.select('span.dataValue[itemprop="height"]')

        if len(player_height_soup) != 0:
            player_fetched_info.append(float(player_height_soup[0].text[:-1].replace(",", ".")))
        else:
            player_fetched_info.append(np.nan)

        player_transfer_date_soup = player_page_soup.select('span.dataItem:-soup-contains("W drużynie od:") + span.dataValue')

        if len(player_transfer_date_soup) != 0:
            player_fetched_info.append(player_transfer_date_soup[0].text)
        else:
            player_fetched_info.append(np.nan)

        player_stats_soup = player_page_soup.select('td:-soup-contains("Łącznie:") ~ td')

        if len(player_stats_soup) != 0:
            player_fetched_info.append(player_stats_soup[1].text)
            player_fetched_info.append(player_stats_soup[2].text)
            player_fetched_info.append(0 if player_position == "BRK" else player_stats_soup[3].text)
            player_fetched_info.append(player_stats_soup[3].text if player_position == "BRK" else player_stats_soup[4].text)
            player_fetched_info.append(player_stats_soup[4].text if player_position == "BRK" else player_stats_soup[5].text)
            player_fetched_info.append(player_stats_soup[5].text if player_position == "BRK" else player_stats_soup[6].text)
            player_fetched_info.append(player_stats_soup[6].text if player_position == "BRK" else player_stats_soup[7].text)
            player_fetched_info.append(player_stats_soup[7].text if player_position == "BRK" else player_stats_soup[8].text)
            player_fetched_info.append(player_stats_soup[8].text if player_position == "BRK" else player_stats_soup[9].text)
            player_fetched_info.append(player_stats_soup[-1].text)
        else:
            player_fetched_info.extend([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

        player_fetched_info = [0 if x == "-" else x for x in player_fetched_info]

        player_info_dict[player_href] = player_fetched_info
        player_info = player_fetched_info

    return player_info


def gather_info_from_file(file_name):
    file = open(file_name, "r", encoding="utf-8")
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
            player_additional_info = get_player_additional_info(player.select('td:nth-child(1) > div > span > a')[0]['href'], player_position_scut)
            player_club_assigned = player.select('td:nth-child(7) > a')[0]['title'] if len(player.select('td:nth-child(7) > a')) > 0 else ""
            player_club_assigned_info = get_club_info(player.select('td:nth-child(7) > a')[0]['href']) if len(player.select('td:nth-child(7) > a')) > 0 else ""
            player_club_assigned_level = player_club_assigned_info[0] if (len(player_club_assigned_info) == 2) else ""
            player_club_assigned_nationality = player_club_assigned_info[1] if (len(player_club_assigned_info) == 2) else ""
            player_compensation = player.select('td:nth-child(9) > a')[0].text.lower()
            player_transfer_or_loan = player.select('td:nth-child(9) > a')[0].text.lower()

            player_rows.append([club_name, league_info[0], league_info[1], player_name, player_age, player_additional_info[0], player_additional_info[1], player_additional_info[2], player_additional_info[3], player_additional_info[4], player_additional_info[5], player_additional_info[6], player_additional_info[7], player_additional_info[8], player_additional_info[9], player_additional_info[10], player_additional_info[11], player_position, player_position_scut, player_nationality, player_transfer_or_loan, bought_or_sold, player_value, player_club_assigned, player_club_assigned_level, player_club_assigned_nationality, player_compensation])

    return player_rows


def flatten_list(list_to_flat):
    return [e for sl in list_to_flat for e in sl]


for index, name in enumerate(file_names):
    transfers.append(gather_info_from_file(name))

transfers = flatten_list(flatten_list(transfers))
transfers_df = pd.DataFrame(transfers, columns=transfers_columns)
transfers_df.to_csv('csv/transfers-data-frame.csv', encoding='utf-8')