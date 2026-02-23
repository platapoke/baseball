import discord
import requests
import schedule
import time
from datetime import date, datetime, timedelta
import pytz
import calendar
import asyncio
import mysql.connector
import matplotlib.pyplot as plt
import pandas as pd
import logging
from aiohttp import ClientConnectorError


# Define your Discord bot token
TOKEN = 'token here'

destination_channel_id = 1230531614157570219
score_channel_id = 1230531555777318912
first5_channel_id = 1258127751106396171
#### TEMP NRFI 1357438428664299541 #####
#rfi_channel_id = 1357438428664299541
rfi_channel_id = 1252042444753604618



# Define the URL base for fetching MLB game data
URL_BASE = "http://statsapi.mlb.com/api/v1/schedule/games/?sportId=1&startDate={start_date}&endDate={end_date}"
URLDOMAIN = "http://statsapi.mlb.com"
URLTODAY = "/api/v1/schedule/games/?sportId=1"

# Initialize a Discord client
intents = discord.Intents().all()
# Pass the intents argument when creating the client
client = discord.Client(intents=intents)

logging.basicConfig(filename='bot.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')

@client.event
async def on_ready():
    logging.info(f"Logged in as {client.user}")
    if not hasattr(client, 'synced'):
        client.synced = True
        schedule.every().day.at("10:15").do(schedule_check_games)
        schedule.every().day.at("10:10").do(schedule_firstfive)
        schedule.every().day.at("11:00").do(schedule_NRFI)
        client.loop.create_task(timer())

@client.event
async def on_message(message):
    if message.content.startswith("!series"):
        await check_games()
    elif message.content.startswith("!mlb"):
        await check_scores()
    elif message.content.startswith("!nrfi"):
        await runsfirstinning()
    elif message.content.startswith("!first5"):
        await firstfiveinnings()

async def check_scores():
    channel = client.get_channel(score_channel_id)
    games = get_games(False)
    today = datetime.today()
    formatted_date = today.strftime("%A, %B %d, %Y")
    if games:
        print(f"Found {len(games)} game(s) {formatted_date}")
        embed = discord.Embed(title=f"{formatted_date}",
                description=f"{len(games)} games found! Today's schedule/results:",
                color=discord.Color.blue())  
        # Send the embed message
        await channel.send(embed=embed) 

        #Load gambling info from ESPN if at least one games was found
        oddsdf = extract_odds_data()
        colorcounter = 1
        for game in games:
            away_team = game['teams']['away']['team']['name']
            home_team = game['teams']['home']['team']['name']
            series_game_number = game['seriesGameNumber']
            total_games_in_series = game['gamesInSeries']
            game_status = game['status']['statusCode']

            game_date_str = game['gameDate']
            game_datetime_utc = datetime.fromisoformat(game_date_str[:-1])  # Remove 'Z' at the end
            local_timezone = pytz.timezone("America/New_York")  # Assuming you're in New York
            game_datetime_local = pytz.utc.localize(game_datetime_utc).astimezone(local_timezone)
            game_time = game_datetime_local.strftime("%-I:%M %p")
            db_game =""
            game_conditions=""
            homeemoji = get_emoji(home_team)
            awayemoji = get_emoji(away_team)  
            title_message = f"{away_team} vs {home_team} - {game['venue']['name']} at {game_time}"
            if game_status =="P" or game_status == "S":
                away_pitcher, home_pitcher, game_weather= get_game_details(game['link'])

                if game['doubleHeader'] != "N":
                    db_game ="__**Doubleheader**__\n"
                # Game weather might not be available yet
                if game_weather != "None, NoneF, None":
                    game_conditions = f"\n*Conditions:* {game_weather}\n"
                else:
                    game_conditions = f"\n"
                # FOR PRE-GAME, PUT GAMBLING INFO INTO SCORE MESSAGE
                # Iterate over DataFrame rows
                for index, row in oddsdf.iterrows():
                    odds_home_away = row['HomeAway']
                    odds_team_name = row['TeamName']
                    odds_team_short = row['TeamShort']
                    odds_moneyline = row['MoneyLine']
                    odds_spread = row['Spread']
                    odds_spread_odds = row['SpreadOdds']
                    odds_total = row['Runline']
                    odds_total_odds = row['Runline Odds']
                    spread_combined = f"{odds_spread} {odds_spread_odds}"
                    total_combined = f"{odds_total} {odds_total_odds}"
                    if odds_team_name == home_team:
                        var5 = odds_team_short
                        var6 = odds_moneyline
                        var7 = spread_combined
                        var8 = total_combined
                    elif odds_team_name == away_team:
                        var1 = odds_team_short
                        var2 = odds_moneyline
                        var3 = spread_combined
                        var4 = total_combined

                table = [['TM','ML','RUNLINE'],[var1, var2, var3], [var5, var6, var7]]
                formatted_table = format_table(table)
                if away_pitcher is None or away_pitcher == '':
                    away_pitcher_record = '' 
                else:
                    away_pitcher_record = get_pitching_record(away_pitcher)
                if home_pitcher is None or home_pitcher == '':
                    home_pitcher_record = ''
                else:    
                    home_pitcher_record = get_pitching_record(home_pitcher)


                away_message = f"{awayemoji} *Away Pitcher:* \n{away_pitcher} {away_pitcher_record}\n"
                home_message = f"{homeemoji} *Home Pitcher:* \n{home_pitcher} {home_pitcher_record}\n"
                score_message = f"```{formatted_table}```"
            elif game_status == 'DR':
                away_message = ""
                home_message = ""
                score_message = "**POSTPONED**"
            else:
                # Handle KeyError gracefully
#                away_score = game['teams']['away'].get('score', 'Score not available')
#                home_score = game['teams']['home'].get('score', 'Score not available')
                
                if game_status == 'F':
                    line_score = "**FINAL**"

                else:
                    line_score = get_score_details(game['link'])
                home_score = (game['teams']['home']['score'])  
                away_score = (game['teams']['away']['score'])                
                away_message = f"{awayemoji}{away_team} **{away_score}**\n"
                home_message = f"{homeemoji}{home_team} **{home_score}**\n"   
                score_message = f"{line_score}"          
            if colorcounter % 2 == 0:
                discordcolor = 0x228B22
            else:
                discordcolor = 0x006400
            colorcounter += 1
            # Send the embed message
            embed = discord.Embed(title=f"{title_message}",
                    description=f"{db_game}{away_message}{home_message}{game_conditions}{score_message}",
                    color=discordcolor)

            await channel.send(embed=embed)

    else:
        embed = discord.Embed(title=f"{formatted_date}",
                description="No games were found. Please check after 10:00am.",
                color=discord.Color.red())
               
            # Send the embed message
        await channel.send(embed=embed) 
        #await channel.send("No relevant games found for today.")

async def runsfirstinning():
    channel = client.get_channel(rfi_channel_id)
    games = get_games(False)
    today = datetime.today()
    formatted_date = today.strftime("%A, %B %d, %Y")
    if games:
        first_day_of_current_year = get_first_day_of_current_year()

        colorcounter = 1  #Counter for discord color change
        mydb = mysql.connector.connect (
                host="localhost",
                user="jack",
                password="draftkings",
                database="stats"
        )       
        mycursor = mydb.cursor()
        sql = f"""SELECT (SUM(CASE WHEN (homescore = 0 AND awayscore = 0) THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS nrfi_percentage FROM runsfirst WHERE date > '{first_day_of_current_year}';"""
        mycursor.execute(sql)

        # Fetch the result
        nrfi = mycursor.fetchone()[0]

        sql2 = f"""SELECT SUM(awayscore + homescore) / (2 * COUNT(*)) as ave_runs FROM runsfirst WHERE date > '{first_day_of_current_year}';"""
        mycursor.execute(sql2)
        ave_runs = mycursor.fetchone()[0]    

        embed = discord.Embed(title=f"{formatted_date}",
                description=f"{len(games)} games found!\n**Current Season NRFI Rate: {nrfi}%**\nCurrent Season Average runs in 1st Inning per team: **{ave_runs}**\nToday's NRFI info:",
                color=discord.Color.blue())  
        # Send the embed message
        await channel.send(embed=embed) 



        for game in games:
            away_team = game['teams']['away']['team']['name']
            home_team = game['teams']['home']['team']['name']
            game_status = game['status']['statusCode']

            away_team_wins = game['teams']['away']['leagueRecord']['wins']
            away_team_losses = game['teams']['away']['leagueRecord']['losses']

            away_record = f"({away_team_wins} - {away_team_losses})"
            home_team_wins = game['teams']['home']['leagueRecord']['wins']
            home_team_losses = game['teams']['home']['leagueRecord']['losses']

            home_record = f"({home_team_wins} - {home_team_losses})"
            game_date_str = game['gameDate']
            game_datetime_utc = datetime.fromisoformat(game_date_str[:-1])  # Remove 'Z' at the end
            local_timezone = pytz.timezone("America/New_York")  # Assuming you're in New York
            game_datetime_local = pytz.utc.localize(game_datetime_utc).astimezone(local_timezone)
            game_time = game_datetime_local.strftime("%-I:%M %p")

            db_game =""

            homeemoji = get_emoji(home_team)
            awayemoji = get_emoji(away_team)  
            title_message = f"{away_team} vs {home_team} at {game_time}"
            away_message = ""
            home_message = ""


            if game_status =="P" or game_status == "S":
                # Alternate discord colors to help distiguish between games
                if colorcounter % 2 == 0:
                    discordcolor = 0x228B22  #Forest Green
                else:
                    discordcolor = 0x006400  #Dark Green
                colorcounter += 1
                away_pitcher, home_pitcher, game_weather= get_game_details(game['link'])

                if game['doubleHeader'] != "N":
                    db_game ="__**Doubleheader**__\n"

                if home_pitcher is None or home_pitcher == '':
                    home_pitcher_statline = ''   
                    home_pitcher_record = ''
                else:    
                    # Define the SQL queries
                    query1 = f"SELECT COUNT(*) FROM runsfirst WHERE homepitcher = '{home_pitcher}' AND date > '{first_day_of_current_year}';"
                    query2 = f"SELECT COUNT(*) FROM runsfirst WHERE homepitcher = '{home_pitcher}' AND awayscore = 0 AND date > '{first_day_of_current_year}';"
                    query3 = f"SELECT COUNT(*) FROM runsfirst WHERE awaypitcher = '{home_pitcher}'AND date > '{first_day_of_current_year}';"
                    query4 = f"SELECT COUNT(*) FROM runsfirst WHERE awaypitcher = '{home_pitcher}' AND homescore = 0 AND date > '{first_day_of_current_year}';"  


                    # HOME PITCHER STATS
                    mycursor.execute(query1)
                    result1 = mycursor.fetchone()[0]
                    hm_p_hm_starts = int(result1)
                    mycursor.execute(query2)
                    result2 = mycursor.fetchone()[0]
                    hm_p_hm_runs = int(result2)
                    mycursor.execute(query3)
                    result3 = mycursor.fetchone()[0]
                    hm_p_aw_starts = int(result3)
                    mycursor.execute(query4)
                    result4 = mycursor.fetchone()[0]
                    hm_p_aw_runs = int(result4)
                    hm_p_tot_starts = hm_p_hm_starts + hm_p_aw_starts
                    hm_p_tot_runs =  hm_p_hm_runs + hm_p_aw_runs
                    home_pitcher_statline = f'Overall opponent NRFI {hm_p_tot_runs} of {hm_p_tot_starts} starts. Opponent NRFI {hm_p_hm_runs} of {hm_p_hm_starts} in home starts.'
                    home_pitcher_record = get_pitching_record(home_pitcher)  

                if away_pitcher is None or away_pitcher == '':
                    away_pitcher_statline = '' 
                    away_pitcher_record = ''  
                else:    
                    # Define the SQL queries
                    query5 = f"SELECT COUNT(*) FROM runsfirst WHERE homepitcher = '{away_pitcher}' AND date > '{first_day_of_current_year}';"
                    query6 = f"SELECT COUNT(*) FROM runsfirst WHERE homepitcher = '{away_pitcher}' AND awayscore = 0 AND date > '{first_day_of_current_year}';"
                    query7 = f"SELECT COUNT(*) FROM runsfirst WHERE awaypitcher = '{away_pitcher}' AND date > '{first_day_of_current_year}';"
                    query8 = f"SELECT COUNT(*) FROM runsfirst WHERE awaypitcher = '{away_pitcher}' AND homescore = 0 AND date > '{first_day_of_current_year}';" 
                
                    # AWAY PITCHER STATS
                    mycursor.execute(query5)
                    result5 = mycursor.fetchone()[0]
                    aw_p_hm_starts = int(result5)
                    mycursor.execute(query6)
                    result6 = mycursor.fetchone()[0]
                    aw_p_hm_runs = int(result6)
                    mycursor.execute(query7)
                    result7 = mycursor.fetchone()[0]
                    aw_p_aw_starts = int(result7)
                    mycursor.execute(query8)
                    result8 = mycursor.fetchone()[0]
                    aw_p_aw_runs = int(result8)
                    aw_p_tot_starts = aw_p_hm_starts + aw_p_aw_starts
                    aw_p_tot_runs =  aw_p_hm_runs + aw_p_aw_runs
                    away_pitcher_statline = f'Overall opponent NRFI {aw_p_tot_runs} of {aw_p_tot_starts} starts. Opponent NRFI {aw_p_aw_runs} of {aw_p_aw_starts} in away starts.'
                    away_pitcher_record = get_pitching_record(away_pitcher)

                # Define the SQL queries
                query13 = f"select count(*) from runsfirst where hometeam = '{home_team}' and (homescore = 0 and awayscore = 0) AND date > '{first_day_of_current_year}';"
                query14 = f"select count(*) from runsfirst where hometeam = '{home_team}' and (homescore > 0 or  awayscore > 0) AND date > '{first_day_of_current_year}';"
                query15 = f"select count(*) from runsfirst where awayteam = '{home_team}' and (homescore = 0 and  awayscore = 0) AND date > '{first_day_of_current_year}';"
                query16 = f"select count(*) from runsfirst where awayteam = '{home_team}' and (homescore > 0 or  awayscore > 0) AND date > '{first_day_of_current_year}';"

                # HOME TEAM NRFI RECORD
                mycursor.execute(query13)
                result13 = mycursor.fetchone()[0]
                hm_nrfi_hm_w = int(result13)
                mycursor.execute(query14)
                result14 = mycursor.fetchone()[0]
                hm_nrfi_hm_l = int(result14)
                mycursor.execute(query15)
                result15 = mycursor.fetchone()[0]
                hm_nrfi_aw_w = int(result15)
                mycursor.execute(query16)
                result16 = mycursor.fetchone()[0]
                hm_nrfi_aw_l = int(result16)

                hm_nrfi_overall_w = hm_nrfi_hm_w + hm_nrfi_aw_w
                hm_nrfi_overall_l = hm_nrfi_hm_l + hm_nrfi_aw_l

                # Safely calculate the overall percentage to avoid division by zero
                if hm_nrfi_overall_w + hm_nrfi_overall_l > 0:
                    hm_nrfi_overall_perc = (hm_nrfi_overall_w / (hm_nrfi_overall_w + hm_nrfi_overall_l)) * 100
                else:
                    hm_nrfi_overall_perc = 0  

                # Safely calculate the away percentage to avoid division by zero
                if hm_nrfi_hm_w + hm_nrfi_hm_l > 0:
                    hm_nrfi_hm_perc = (hm_nrfi_hm_w / (hm_nrfi_hm_w + hm_nrfi_hm_l)) * 100
                else:
                    hm_nrfi_hm_perc = 0  


                #hm_nrfi_overall_perc = (hm_nrfi_overall_w / (hm_nrfi_overall_w + hm_nrfi_overall_l)) * 100
                #hm_nrfi_hm_perc = (hm_nrfi_hm_w / (hm_nrfi_hm_w + hm_nrfi_hm_l)) * 100
                home_nrfi = f'Overall NRFI ({hm_nrfi_overall_w} - {hm_nrfi_overall_l}) {hm_nrfi_overall_perc:.2f}%\nHome NFRI ({hm_nrfi_hm_w} - {hm_nrfi_hm_l}) {hm_nrfi_hm_perc:.2f}%'
                # Call 1st Inning Runs per game average



                home_runs_ave, home_runs_away = firstrunsaverage (home_team, 'home')
                home_average_message = f"\n:small_blue_diamond:__**1st Inning Runs Scored**__ \nAverage Per Game: {home_runs_ave}\nAverage at Home: {home_runs_away}\n:small_blue_diamond:__**NRFI Record**__"

                # Define the SQL queries
                query17 = f"select count(*) from runsfirst where hometeam = '{away_team}' and (homescore = 0 and awayscore = 0) AND date > '{first_day_of_current_year}';"
                query18 = f"select count(*) from runsfirst where hometeam = '{away_team}' and (homescore > 0 or  awayscore > 0) AND date > '{first_day_of_current_year}';"
                query19 = f"select count(*) from runsfirst where awayteam = '{away_team}' and (homescore = 0 and  awayscore = 0) AND date > '{first_day_of_current_year}';"
                query20 = f"select count(*) from runsfirst where awayteam = '{away_team}' and (homescore > 0 or  awayscore > 0) AND date > '{first_day_of_current_year}';"

                # AWAY TEAM NRFI RECORD
                mycursor.execute(query17)
                result17 = mycursor.fetchone()[0]
                aw_nrfi_hm_w = int(result17)
                mycursor.execute(query18)
                result18 = mycursor.fetchone()[0]
                aw_nrfi_hm_l = int(result18)           
                mycursor.execute(query19)
                result19 = mycursor.fetchone()[0]
                aw_nrfi_aw_w = int(result19)         
                mycursor.execute(query20)
                result20 = mycursor.fetchone()[0]
                aw_nrfi_aw_l = int(result20)
          
                aw_nrfi_overall_w = aw_nrfi_hm_w + aw_nrfi_aw_w
                aw_nrfi_overall_l = aw_nrfi_hm_l + aw_nrfi_aw_l

                # Safely calculate the overall percentage to avoid division by zero
                if aw_nrfi_overall_w + aw_nrfi_overall_l > 0:
                    aw_nrfi_overall_perc = (aw_nrfi_overall_w / (aw_nrfi_overall_w + aw_nrfi_overall_l)) * 100
                else:
                    aw_nrfi_overall_perc = 0  

                # Safely calculate the away percentage to avoid division by zero
                if aw_nrfi_aw_w + aw_nrfi_aw_l > 0:
                    aw_nrfi_aw_perc = (aw_nrfi_aw_w / (aw_nrfi_aw_w + aw_nrfi_aw_l)) * 100
                else:
                    aw_nrfi_aw_perc = 0  


                #aw_nrfi_overall_perc = (aw_nrfi_overall_w / (aw_nrfi_overall_w + aw_nrfi_overall_l)) * 100
                #aw_nrfi_aw_perc = (aw_nrfi_aw_w / (aw_nrfi_aw_w + aw_nrfi_aw_l)) * 100


                away_nrfi = f'Overall NRFI ({aw_nrfi_overall_w} - {aw_nrfi_overall_l}) {aw_nrfi_overall_perc:.2f}%\nAway NFRI ({aw_nrfi_aw_w} - {aw_nrfi_aw_l}) {aw_nrfi_aw_perc:.2f}%'

                # Call 1st Inning Runs per game average
                away_runs_ave, away_runs_away = firstrunsaverage (away_team, 'away')
                away_average_message = f"\n:small_orange_diamond:__**1st Inning Runs Scored**__ \nAverage Per Game: {away_runs_ave}\nAverage Away: {away_runs_away}\n:small_orange_diamond:__**NRFI Record**__"

                away_message = f"{awayemoji} **{away_team}** {away_record}{away_average_message}\n{away_nrfi}\n:small_orange_diamond:__**Pitcher**__ \n{away_pitcher} {away_pitcher_record}\n{away_pitcher_statline}\n\n"
                home_message = f"{homeemoji} **{home_team}** {home_record}{home_average_message}\n{home_nrfi}\n:small_blue_diamond:__**Pitcher**__ \n{home_pitcher} {home_pitcher_record}\n{home_pitcher_statline}"


                # Send the embed message
                embed = discord.Embed(title=f"{title_message}",
                        description=f"{db_game}{away_message}{home_message}",
                        color=discordcolor)

                await channel.send(embed=embed)
                
        # Close the cursor and connection
        mycursor.close()
        mydb.close()

async def firstfiveinnings():
    channel = client.get_channel(first5_channel_id)
    games = get_games(False)
    today = datetime.today()
    formatted_date = today.strftime("%A, %B %d, %Y")
    if games:

        colorcounter = 1  #Counter for discord color change
        embed = discord.Embed(title=f"{formatted_date}",
        description=f"{len(games)} games found! Today's First Five schedule:",
        color=discord.Color.blue())  
        # Send the embed message
        await channel.send(embed=embed) 
        for game in games:
            away_team = game['teams']['away']['team']['name']
            home_team = game['teams']['home']['team']['name']
            game_status = game['status']['statusCode']
            game_date_str = game['gameDate']
            game_datetime_utc = datetime.fromisoformat(game_date_str[:-1])  # Remove 'Z' at the end
            local_timezone = pytz.timezone("America/New_York")  # Assuming you're in New York
            game_datetime_local = pytz.utc.localize(game_datetime_utc).astimezone(local_timezone)
            game_time = game_datetime_local.strftime("%-I:%M %p")
            homeemoji = get_emoji(home_team)
            awayemoji = get_emoji(away_team)  
            title_message = f"{away_team} vs {home_team} at {game_time}"
            top_message = f"__**FIRST FIVE INNINGS**__\n"
            away_message = ""
            home_message = ""
            if game_status =="P" or game_status == "S":
                # Alternate discord colors to help distiguish between games
                if colorcounter % 2 == 0:
                    discordcolor = 0x228B22  #Forest Green
                else:
                    discordcolor = 0x006400  #Dark Green
                colorcounter += 1
                a_homeruns, a_home_runattempts, a_awayruns, a_away_runattempts, a_averageallruns = get_firstfivestats (away_team, 'runs')
                #a_averageruns = a_awayruns/a_away_runattempts
                h_homeruns, h_home_runattempts, h_awayruns, h_away_runattempts, h_averageallruns = get_firstfivestats (home_team, 'runs')
                #h_averageruns = h_homeruns/h_home_runattempts

                a_homehits, a_home_hitattempts, a_awayhits, a_away_hitattempts, a_averageallhits = get_firstfivestats (away_team, 'hits')
                #a_averagehits = a_awayhits/a_away_hitattempts
                h_homehits, h_home_hitattempts, h_awayhits, h_away_hitattempts, h_averageallhits = get_firstfivestats (home_team, 'hits')
                #h_averagehits = h_homehits/h_home_hitattempts
                
                a_homelob, a_home_lobattempts, a_awaylob, a_away_lobattempts, a_averagealllob = get_firstfivestats (away_team, 'lob')
                #a_averagelob = a_awaylob/a_away_lobattempts
                h_homelob, h_home_lobattempts, h_awaylob, h_away_lobattempts, h_averagealllob = get_firstfivestats (home_team, 'lob')
                #h_averagelob = h_homelob/h_home_lobattempts

                # Check for division by zero
                a_averageruns = a_awayruns / a_away_runattempts if a_away_runattempts != 0 else 0.0
                h_averageruns = h_homeruns / h_home_runattempts if h_home_runattempts != 0 else 0.0

                a_averagehits = a_awayhits / a_away_hitattempts if a_away_hitattempts != 0 else 0.0
                h_averagehits = h_homehits / h_home_hitattempts if h_home_hitattempts != 0 else 0.0

                a_averagelob = a_awaylob / a_away_lobattempts if a_away_lobattempts != 0 else 0.0
                h_averagelob = h_homelob / h_home_lobattempts if h_home_lobattempts != 0 else 0.0


                away_runline = f"Ave Runs: {a_averageallruns:.2f}, Away: {a_averageruns:.2f}\n"
                away_hitline = f"Ave Hits: {a_averageallhits:.2f}, Away: {a_averagehits:.2f}\n"
                away_lobline = f"Ave LOB: {a_averagealllob:.2f}, Away: {a_averagelob:.2f}\n"
                home_runline = f"Ave Runs: {h_averageallruns:.2f}, Home: {h_averageruns:.2f}\n"
                home_hitline = f"Ave Hits: {h_averageallhits:.2f}, Home: {h_averagehits:.2f}\n"
                home_lobline = f"Ave LOB: {h_averagealllob:.2f}, Home: {h_averagelob:.2f}\n"
                away_message = f'{awayemoji}\n{away_runline}{away_hitline}{away_lobline}'
                home_message = f'{homeemoji}\n{home_runline}{home_hitline}{home_lobline}'
                # Send the embed message
                embed = discord.Embed(title=f"{title_message}",
                        description=f"{top_message}{away_message}{home_message}",
                        color=discordcolor)

                await channel.send(embed=embed)


async def check_games():
    channel = client.get_channel(destination_channel_id)
    pgames = getprior_games()
    today = datetime.today()
    formatted_date = today.strftime("%A, %B %d, %Y")
    if pgames:
        print(f"Found {len(pgames)} prior games for {formatted_date}")
        #call get games with true because we want only the series ending games, not all games
        games = get_games(True)
        first_day_of_current_year = get_first_day_of_current_year()

        mydb = mysql.connector.connect (
                    host="localhost",
                    user="jack",
                    password="draftkings",
                    database="stats"
        )       
        mycursor = mydb.cursor()
        # Get this seasons skunk avoidance rate
        sql0 = f"""SELECT (COUNT(CASE WHEN skunk_avoided = 1 THEN 1 END) / COUNT(*)) * 100 AS skunk_avoidance_percentage FROM skunk_table where date > '{first_day_of_current_year}';"""
        mycursor.execute(sql0)

        # Fetch the result. If the fetch fails OR is first games of year, set to zero.  Otherwise, discord message will fail if NONE.
        avoidance = mycursor.fetchone()[0] or 0.0000

        if games:
 #           first_day_of_current_year = get_first_day_of_current_year()

            colorcounter = 1  #Counter for discord color change
 #           mydb = mysql.connector.connect (
 #                   host="localhost",
 #                   user="jack",
 #                   password="draftkings",
 #                   database="stats"
 #           )       
 #           mycursor = mydb.cursor()
 #           # Get this seasons skunk avoidance rate
 #           sql0 = f"""SELECT (COUNT(CASE WHEN skunk_avoided = 1 THEN 1 END) / COUNT(*)) * 100 AS skunk_avoidance_percentage FROM skunk_table where date > '{first_day_of_current_year}';"""
 #           mycursor.execute(sql0)
 #           # Fetch the result. If the fetch fails OR is first games of year, set to zero.  Otherwise, discord message will fail if NONE.
 #           avoidance = mycursor.fetchone()[0] or 0.0000

            #Load gambling info from ESPN if at least one games was found
            oddsdf = extract_odds_data()
            numofseriesgames = len(games)
            print(f"Found {numofseriesgames} series ending game(s) {formatted_date} Avoidance = {avoidance}")
            embed = discord.Embed(title=f"{formatted_date}",
                    description=f"Current Season Skunk Avoidance Rate is **{avoidance:.2f}%**\nThe following series game(s) found today:",
                    color=discord.Color.blue())
               
            # Send the embed message
            await channel.send(embed=embed)  
            series_skunks = 0
            # Connect to the database
            colorcounter = 1
            for game in games:
                game_id = game['gamePk']
                away_team = game['teams']['away']['team']['name']
                away_wins = game['teams']['away']['leagueRecord']['wins']
                away_losses = game['teams']['away']['leagueRecord']['losses']
                away_pct = game['teams']['away']['leagueRecord']['pct']
                away_record = f"({away_wins} - {away_losses})"
                home_wins = game['teams']['home']['leagueRecord']['wins']
                home_losses = game['teams']['home']['leagueRecord']['losses']
                home_pct = game['teams']['home']['leagueRecord']['pct']
                home_record = f"({home_wins} - {home_losses})"
                home_team = game['teams']['home']['team']['name']
                home_team_seriesnum = game['teams']['home']['seriesNumber']
                series_game_number = game['seriesGameNumber']
                total_games_in_series = game['gamesInSeries']
                game_date_str = game['gameDate']
                game_datetime_utc = datetime.fromisoformat(game_date_str[:-1])  # Remove 'Z' at the end
                local_timezone = pytz.timezone("America/New_York")  # Assuming you're in New York
                game_datetime_local = pytz.utc.localize(game_datetime_utc).astimezone(local_timezone)
                game_time = game_datetime_local.strftime("%-I:%M %p")
                today_date = date.today() 
                game_season = game['seriesDescription']
                away_pitcher, home_pitcher, game_weather= get_game_details(game['link'])
                db_game =""
                game_conditions=""
                if colorcounter % 2 == 0:
                    discordcolor = 0x228B22
                else:
                    discordcolor = 0x006400
                colorcounter += 1
                if game['doubleHeader'] != "N":
                    db_game ="__**2nd Game of Doubleheader**__\n:warning: See Results of 1st Game. This game may no longer meet criteria.:warning:\n"
                    doubleheader = True
                else:
                    doubleheader = False
                # Game weather might not be available yet
                if game_weather != "None, NoneF, None":
                    game_conditions = f"\n*Conditions:* {game_weather}"

                # Initialize a counter for team wins
                home_team_wins = 0
                away_team_wins = 0

                # Iterate through prior games
                for pg in pgames:
                    prior_home_team = pg['teams']['home']['team']['name']
                    prior_away_team = pg['teams']['away']['team']['name']
              
                    # Check if the home team in the prior game matches the home team in the current game
                    if prior_home_team == home_team and prior_away_team == away_team:
                        # Check if the home team won the prior game

                        if pg['teams']['home']['isWinner']:
                            home_team_wins += 1
                        else:
                            away_team_wins += 1
                            
                # Display only end of series game where one team hasn't won
                if home_team_wins == 0 or away_team_wins == 0:
                    for index, row in oddsdf.iterrows():
                        # All of these are defined merely to show to contents of the row in the dataframe
                        # for future reference or use.
                        # Important:  the odds_runline, odds_runline_odds for the away team are the over
                        #             and the home team will show the under for the game.  The games are actually paired
                        #             inside the detaframe.  Example: rows [0],[1] are one game, [2],[3] another game, and so on.
                        odds_home_away = row['HomeAway']
                        odds_team_name = row['TeamName']
                        odds_moneyline = row['MoneyLine']
                        odds_spread = row['Spread']
                        odds_spread_odds = row['SpreadOdds']
                        odds_runline = row['Runline']
                        odds_runline_odds = row['Runline Odds']
                        if odds_team_name == home_team:
                            home_team_moneyline = odds_moneyline
                        elif odds_team_name == away_team:
                            away_team_moneyline = odds_moneyline

                    series_skunks += 1

                    #ONLY ADD REGULAR SEASON GAMES TO THE DATABASE
                    if game_season == 'Regular Season':
                        try:
                            
                            sql = "INSERT INTO possible_skunks (gameid, date, hometeam, homepct, homewins, awayteam, awaypct, awaywins, gamesinseries, doubleheader, hometeamseriesnum) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            # Tuple of values to insert
                            values = (game_id, today_date, home_team, home_pct, home_team_wins, away_team, away_pct, away_team_wins, total_games_in_series, doubleheader, home_team_seriesnum)

                            # Execute the SQL query
                            mycursor.execute(sql, values)

                            mydb.commit()
                        except mysql.connector.IntegrityError as e:
                            # Handle the case where the insertion failed due to a unique constraint violation
                            print("Insertion failed:", e)
                            mydb.rollback()  # Rollback the transaction to maintain data integrity
                        except Exception as e:
                            # Handle other exceptions
                            print("An error occurred:", e)

                    away_skunk_record = ''
                    home_skunk_record = ''
                    first_day_of_current_year = get_first_day_of_current_year()
                    if home_team_wins == 0:
                        skunk_team = home_team
                    else:
                        skunk_team = away_team
                    try:
                        query1 = f"""
                            SELECT 
                                COUNT(CASE WHEN skunk_avoided THEN 1 END) AS avoided,
                                COUNT(*) AS attempts
                            FROM skunk_table
                            WHERE 
                                date > '{first_day_of_current_year}' AND
                                (
                                    (hometeam = '{skunk_team}' AND team_avoiding = 'home') OR
                                    (awayteam = '{skunk_team}' AND team_avoiding = 'away')
                                );
                                """
                        # Execute the SQL query
                        mycursor.execute(query1)
                        query1result = mycursor.fetchone()
                        skunks = query1result[0]
                        attempts = query1result[1]
                        if home_team_wins == 0:
                            home_skunk_record = f":bulb: Avoided {skunks} of {attempts} skunks this year\n"
                        else:
                            away_skunk_record = f":bulb: Avoided {skunks} of {attempts} skunks this year\n"
                    except Exception as e:
                        # Handle other exceptions
                        print("An error occurred:", e)


                    # Create an embed object
                     # Form the embed message
                    homeemoji = get_emoji(home_team)
                    awayemoji = get_emoji(away_team)
                    betemoji = get_emoji('espnbet')
                    if away_pitcher is None or away_pitcher == '':
                        away_pitcher_record = '' 
                    else:
                        away_pitcher_record = get_pitching_record(away_pitcher)
                    if home_pitcher is None or home_pitcher == '':
                        home_pitcher_record = ''
                    else:    
                        home_pitcher_record = get_pitching_record(home_pitcher)

                    title_message = f"Game {series_game_number} of {total_games_in_series}: {game['venue']['name']} at {game_time}"
                    away_message = f"{awayemoji}**{away_team}** {away_record}\n({away_team_wins} wins in series)\n{away_skunk_record}*Pitcher:* {away_pitcher} {away_pitcher_record}\n{betemoji} *Moneyline:* {away_team_moneyline}\n\n"
                    home_message = f"{homeemoji}**{home_team}** {home_record}\n({home_team_wins} wins in series)\n{home_skunk_record}*Pitcher:* {home_pitcher} {home_pitcher_record}\n{betemoji} *Moneyline:* {home_team_moneyline}\n"
                    score_message = ""

                    # At this point, we know either the home team has 0 wins or the away team has zero wins
                    if home_team_wins == 0 and home_pct > away_pct:
                        score_message = "\n:rotating_light: **Best Criteria Bet!** :rotating_light:"
                    # Send the embed message
                    embed = discord.Embed(title=f"{title_message}",
                          description=f"{db_game}{away_message}{home_message}{game_conditions}{score_message}",
                          color=discordcolor)
               
                    # Send the embed message
                    await channel.send(embed=embed)
            if series_skunks == 0:
                embed = discord.Embed(title=f"{formatted_date}",
                    description=f"Series ending games were found, but there were no skunks! \nCurrent Season Skunk Avoidance Rate is **{avoidance:.2f}%**",
                    color=discord.Color.red())
                await channel.send(embed=embed)
            # Close the cursor and the database connection
            mycursor.close()
            mydb.close()
        else:
            embed = discord.Embed(title=f"{formatted_date}",
                    description=f"No Relevant Series games found. \nCurrent Season Skunk Avoidance Rate is **{avoidance:.2f}%**",
                    color=discord.Color.red())
               
            # Send the embed message
            await channel.send(embed=embed)  
            #await channel.send("No relevant games found for today.")

    else:
        embed = discord.Embed(title=f"{formatted_date}",
                description="No Prior week games found.",
                color=discord.Color.red())
               
            # Send the embed message
        await channel.send(embed=embed) 
        #await channel.send("No prior games found.")

def schedule_check_games():
    asyncio.ensure_future(check_games())

def schedule_NRFI():
    asyncio.ensure_future(runsfirstinning())

def schedule_firstfive():
    asyncio.ensure_future(firstfiveinnings())

async def timer():
    while True:
        schedule.run_pending()
        await asyncio.sleep(15)

def get_first_day_of_current_year():
    current_year = datetime.now().year
    first_day = datetime(current_year, 1, 1)
    return first_day.strftime('%Y-%m-%d')

# Function to format the table as a string
def format_table(table):
    formatted_table = ""
    for row in table:
        formatted_table += "{:<4} {:<5} {:<10}\n".format(*row)
    return formatted_table

def firstrunsaverage (team, homeoraway):
    first_day_of_current_year = get_first_day_of_current_year()
    mydb = mysql.connector.connect (
                host="localhost",
                user="jack",
                password="draftkings",
                database="stats"
        )       
    mycursor = mydb.cursor()
    query1 = f"""SELECT 
                (SUM(CASE WHEN hometeam = '{team}' THEN homescore ELSE 0 END) +
                SUM(CASE WHEN awayteam = '{team}' THEN awayscore ELSE 0 END)) /
                COUNT(*) AS runs_per_game
            FROM 
                runsfirst
            WHERE 
                (hometeam = '{team}' OR awayteam = '{team}') AND date > '{first_day_of_current_year}';
            """
    mycursor.execute(query1)

    # Fetch the result
    overallave = mycursor.fetchone()[0]

    if homeoraway == 'away':
        query2 = f"""SELECT 
                SUM(CASE WHEN awayteam = '{team}' THEN awayscore ELSE 0 END) /
                COUNT(*) AS runs_per_game
            FROM 
                runsfirst
            WHERE 
                awayteam = '{team}' AND date > '{first_day_of_current_year}';
            """
    else:
        query2 = f"""SELECT 
                SUM(CASE WHEN hometeam = '{team}' THEN homescore ELSE 0 END) /
                COUNT(*) AS runs_per_game
            FROM 
                runsfirst
            WHERE 
                hometeam = '{team}' AND date > '{first_day_of_current_year}';
            """
    mycursor.execute(query2)

    # Fetch the result
    aveforhomeoraway = mycursor.fetchone()[0]

    mycursor.close()
    mydb.close()   
    
    return overallave, aveforhomeoraway


def get_firstfivestats(team, stat):
    first_day_of_current_year = get_first_day_of_current_year()

    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="jack",
            password="draftkings",
            database="stats"
        )
        mycursor = mydb.cursor()

        query = f"""
            SELECT 
                {stat}home, home_attempts, {stat}away, away_attempts, 
                ({stat}home + {stat}away) / (home_attempts + away_attempts) AS average_{stat}
            FROM (
                SELECT 
                    SUM(CASE WHEN hometeam = %s THEN h1_{stat} + h2_{stat} + h3_{stat} + h4_{stat} + h5_{stat} ELSE 0 END) AS {stat}home,
                    SUM(CASE WHEN awayteam = %s THEN a1_{stat} + a2_{stat} + a3_{stat} + a4_{stat} + a5_{stat} ELSE 0 END) AS {stat}away,
                    COUNT(CASE WHEN hometeam = %s THEN 1 END) AS home_attempts,
                    COUNT(CASE WHEN awayteam = %s THEN 1 END) AS away_attempts
                FROM firstfive
                WHERE (hometeam = %s OR awayteam = %s) AND date > %s
            ) AS combined_results;
        """
        params = (team, team, team, team, team, team, first_day_of_current_year)
        mycursor.execute(query, params)

        # Fetch the result
        result = mycursor.fetchone()

        if result:
            stathome = result[0]
            home_attempts = result[1]
            stataway = result[2]
            away_attempts = result[3]
            stataverage = result[4]
        else:
            stathome = None
            home_attempts = None
            stataway = None
            away_attempts = None
            stataverage = None

        return stathome, home_attempts, stataway, away_attempts, stataverage

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None, None, None, None, None

    finally:
        if 'mycursor' in locals() and mycursor:
            mycursor.close()
        if 'mydb' in locals() and mydb:
            mydb.close()


def extract_odds_data():
    # Get today's date and format it as 'yyyymmdd'
    today = datetime.today()
    dateStr = today.strftime('%Y%m%d')

    # Set the URL and headers for the request
    url = "https://site.web.api.espn.com/apis/v2/scoreboard/header"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }
    payload = {
        'sport': 'baseball',
        'league': 'mlb',
        'region': 'us',
        'lang': 'en',
        'contentorigin': 'espn',
        'buyWindow': '1m',
        'showAirings': 'buy,live,replay',
        'tz': 'America/New_York',
        'dates': dateStr
    }

    # Make the request and get the events
    response = requests.get(url, headers=headers, params=payload).json()
    events = response['sports'][0]['leagues'][0]['events']
    
    data = []

    for event in events:
        competitors = event.get('competitors', [])
        odds = event.get('odds', {})

        # Helper function to extract odds safely
        def safe_get(d, keys, default=None):
            for key in keys:
                d = d.get(key, {})
            return d if d else default

        away_moneyline = safe_get(odds, ['moneyline', 'away', 'close', 'odds'])
        home_moneyline = safe_get(odds, ['moneyline', 'home', 'close', 'odds'])
        runline_over = safe_get(odds, ['total', 'over', 'close', 'line'])
        runline_over_odds = safe_get(odds, ['total', 'over', 'close', 'odds'])
        away_spread_odds = safe_get(odds, ['pointSpread', 'away', 'close', 'odds'])
        away_spread = safe_get(odds, ['pointSpread', 'away', 'close', 'line'])
        runline_under = safe_get(odds, ['total', 'under', 'close', 'line'])
        runline_under_odds = safe_get(odds, ['total', 'under', 'close', 'odds'])
        home_spread_odds = safe_get(odds, ['pointSpread', 'home', 'close', 'odds'])
        home_spread = safe_get(odds, ['pointSpread', 'home', 'close', 'line'])
        away_team_short = safe_get(odds, ['awayTeamOdds', 'team','abbreviation'])
        home_team_short = safe_get(odds, ['homeTeamOdds', 'team','abbreviation'])
        away_team = next((team for team in competitors if team['homeAway'] == 'away'), {})
        home_team = next((team for team in competitors if team['homeAway'] == 'home'), {})
        
        data.append({
            'HomeAway': 'away',
            'TeamName': away_team.get('displayName'),
            'TeamShort': away_team_short,
            'MoneyLine': away_moneyline,
            'Spread': away_spread,
            'SpreadOdds': away_spread_odds,
            'Runline': runline_over,
            'Runline Odds': runline_over_odds
        })
        data.append({
            'HomeAway': 'home',
            'TeamName': home_team.get('displayName'),
            'TeamShort': home_team_short,
            'MoneyLine': home_moneyline,
            'Spread': home_spread,
            'SpreadOdds': home_spread_odds,
            'Runline': runline_under,
            'Runline Odds': runline_under_odds            
        })

    df = pd.DataFrame(data)
    return df

def get_pitching_record(pitcher):
    mydb = mysql.connector.connect (
                host="localhost",
                user="jack",
                password="draftkings",
                database="stats"
        )       
    mycursor = mydb.cursor()
    record = ''
    first_day_of_current_year = get_first_day_of_current_year()
    try:
        query1 = f"SELECT COUNT(*) FROM runsfirst WHERE date > '{first_day_of_current_year}' and winner = '{pitcher}';" 
        query2 = f"SELECT COUNT(*) FROM runsfirst WHERE date > '{first_day_of_current_year}' and loser = '{pitcher}';" 
        mycursor.execute(query1)
        wins = mycursor.fetchone()[0]
        mycursor.execute(query2)
        losses = mycursor.fetchone()[0]
        record = f"({wins} - {losses})"
    except Exception as e:
        print("An error occurred:", e)
    mycursor.close()
    mydb.close()    
    return (record)

def get_emoji(team):
    emoji = {
        "Arizona Diamondbacks":"<:dbacks:1242940658814161016>",
        "Atlanta Braves":"<:braves:1243185126867861654>",
        "Baltimore Orioles":"<:orioles:1243185421760987248>",
        "Boston Red Sox":"<:redsox:1243183496235716628>",
        "Chicago Cubs":"<:cubs:1242933131602956348>",
        "Chicago White Sox":"<:whitesox:1243185693027729478>",
        "Cincinnati Reds":"<:reds:1243183591005884506>",
        "Cleveland Guardians":"<:indians:1243183686439010376>",
        "Colorado Rockies":"<:rockies:1243186303432917015>",
        "Detroit Tigers":"<:tigers:1243183813237014691>",
        "Houston Astros":"<:astros:1243183877464264766>",
        "Kansas City Royals":"<:royals:1243183969512591381>",
        "Los Angeles Angels":"<:angels:1243184038907347095>",
        "Los Angeles Dodgers":"<:dodgers:1243184113071161427>",
        "Miami Marlins":"<:marlins:1243186694090522684>",
        "Milwaukee Brewers":"<:brewers:1243187165706584151>",
        "Minnesota Twins":"<:twins:1243187579226951803>",
        "New York Mets":"<:mets:1243187848971026514>",
        "New York Yankees":"<:yanks:1243188081688051762>",
        "Athletics":"<:as:1243188282255212575>",
        "Philadelphia Phillies":"<:phils:1243184214870986814>",
        "Pittsburgh Pirates":"<:bucs:1243184303777779793>",
        "San Diego Padres":"<:padres:1243184371624579082>",
        "San Francisco Giants":"<:giants:1242934406440554636>",
        "Seattle Mariners":"<:mariners:1243188577702248499>",
        "St. Louis Cardinals":"<:cards:1243188879675232256>",
        "Tampa Bay Rays":"<:rays:1243184535634575540>",
        "Texas Rangers":"<:rangers:1243189078904799242>",
        "Toronto Blue Jays":"<:jays:1243189468375158784>",
        "Washington Nationals":"<:nats:1243189513933688932>",
        "espnbet":"<:espnbet:1249895699651498004>"
    }
    teamemoji = emoji.get(team)
    return teamemoji


def getprior_games():
    try:
        # Calculate start and end dates
        end_date = datetime.now() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(weeks=1)     # A week before yesterday

        # Format dates in YYYY-MM-DD
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Construct URL with dynamic dates
        url = URL_BASE.format(start_date=start_date_str, end_date=end_date_str)

        # Fetch game data
        response = requests.get(url)
        data = response.json()

        # Filter games
        priorgames = []
        # Don't add to prior games when missing data (example: postponed game)
        for pdate in data.get('dates', []):
            for game in pdate.get('games', []):
                if 'teams' in game and 'home' in game['teams'] and 'isWinner' in game['teams']['home']:
                    priorgames.append(game)
        return priorgames

#        for pdate in data.get('dates', []):
#            for game in pdate.get('games', []):
#                priorgames.append(game)
#        return priorgames
    except Exception as e:
        print("Error fetching or parsing data:", e)
        return None

#gather more information about a specific game using link from game content
def get_game_details(game_link):
    try:
        # Fetch the JSON data
        response = requests.get(URLDOMAIN + game_link)
        data = response.json()

        # Extract relevant information
        gameinfo= data.get('gameData', {})
        away_pitcher = gameinfo.get('probablePitchers',{}).get('away', {}).get('fullName')
        home_pitcher = gameinfo.get('probablePitchers',{}).get('home', {}).get('fullName')
        weather1 = gameinfo.get('weather', {}).get('condition')
        weather2 = gameinfo.get('weather', {}).get('temp')
        weather3 = gameinfo.get('weather', {}).get('wind')
        game_weather =f'{weather1}, {weather2}F, {weather3}'
        return away_pitcher, home_pitcher, game_weather
    except Exception as e:
        print("Error fetching or parsing game details:", e)
        return None, None, None

def get_score_details(game_link):
    try:
        # Fetch the JSON data
        response = requests.get(URLDOMAIN + game_link)
        data = response.json()

        # Extract relevant information
        gameinfo= data.get('liveData', {})
        inning_state = gameinfo.get('linescore',{}).get('inningState')
        game_inning = gameinfo.get('linescore',{}).get('currentInningOrdinal')
        inning_outs = gameinfo.get('linescore',{}).get('outs')
        if inning_outs == 1:
            out_plural = "out"
        else:
            out_plural = "outs"

        if inning_state == 'Middle' or inning_state == 'End':
            inning_message =""
        else:
            inning_message =f", *{inning_outs} {out_plural}*"    
        line_score = f'{inning_state} {game_inning}{inning_message}'
        return line_score
    except Exception as e:
        print("Error fetching or parsing game details:", e)
        return None
 
 
#get games scheduled for today
def get_games(series):
    try:
        # Calculate start and end dates
        todays_date = datetime.now()   # today
        
        # Format dates in YYYY-MM-DD
        todaysdate_str = todays_date.strftime("%Y-%m-%d")
        response = requests.get(URLDOMAIN+URLTODAY)
        data = response.json()
        games = []
        for date in data.get('dates', []):
            if date['date'] == todaysdate_str:
                for game in date.get('games', []):
                    if 'seriesGameNumber' in game and 'gamesInSeries' in game:
                        #if series, parse out only those games in the last game of the series
                        if series:
                            if game['seriesGameNumber'] == game['gamesInSeries'] or game['seriesGameNumber'] > game['gamesInSeries']:
                                # IF gamesInSeries is less than 3, we're not interested
                                # Note that this could create a situation where the print statement has
                                #   a different number of games found than were sent to discord.
                                if game['gamesInSeries'] > 2:
                                    games.append(game)
                        else:
                            #if not series, send back whole schedule for the day
                            games.append(game)
            else:    
                print(f"Date of today's game file {date['date']}")
        return games
    except Exception as e:
        print("Error fetching or parsing data:", e)
        return None


async def start_bot():
    backoff = 1
    while True:
        try:
            await client.start(TOKEN)
        except (ClientConnectorError, discord.GatewayNotFound, discord.HTTPException, discord.ConnectionClosed) as e:
            logging.error(f"An error occurred: {e}. Reconnecting in {backoff} seconds.")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)  # Exponential backoff with a max of 60 seconds
        except (discord.LoginFailure, discord.Forbidden) as e:
            logging.error(f"Fatal error: {e}. Exiting.")
            break

loop = asyncio.get_event_loop()
loop.run_until_complete(start_bot())
