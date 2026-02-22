import requests
import mysql.connector
import pandas as pd
import warnings
import schedule
import time as tm
from datetime import date, datetime, timedelta
from urllib3.exceptions import NotOpenSSLWarning

warnings.filterwarnings("ignore", category=NotOpenSSLWarning)

skunk_run_time = "10:05"
rfi_run_time = "10:20"
first_five_time = "10:00"
print(f'Here we go! \n * FIRST_FIVE at {first_five_time} \n * MOVE2SKUNK at {skunk_run_time} \n * CHECK_RFI at {rfi_run_time}')


# Define the URL base for fetching MLB game data
URL_BASE = "http://statsapi.mlb.com/api/v1/schedule/games/?sportId=1&startDate={start_date}&endDate={end_date}"
URLDOMAIN = "http://statsapi.mlb.com"
URLTODAY = "/api/v1/schedule/games/?sportId=1"


def first_five():
    games = yesterdays_games()
    today = datetime.today()

    if games:
        possiblerecords = len(games)
        #call get games with true because we want only the series ending games, not all games
        # Connect to the database
        mydb = mysql.connector.connect (
            host="localhost",
            user="jack",
            password="draftkings",
            database="stats"
        )       
        mycursor = mydb.cursor()
        processedrecords = 0
        for game in games:
            game_id = game['gamePk']
            away_team = game['teams']['away']['team']['name']
            home_team = game['teams']['home']['team']['name']
            game_date_str = game['officialDate']
            game_link = game['link']
            game_season = game['seriesDescription']
            response = requests.get(URLDOMAIN + game_link)
            data = response.json()

            gameinfo = data.get('gameData', {})
            scoreinfo = data.get('liveData', {})
            linescore = scoreinfo.get('linescore', {})
            
            innings = linescore.get('innings', [])
            
            # Initialize inning stats
            inning_stats = {
                'h1_runs': None, 'h1_hits': None, 'h1_lob': None, 
                'a1_runs': None, 'a1_hits': None, 'a1_lob': None,
                'h2_runs': None, 'h2_hits': None, 'h2_lob': None, 
                'a2_runs': None, 'a2_hits': None, 'a2_lob': None,
                'h3_runs': None, 'h3_hits': None, 'h3_lob': None, 
                'a3_runs': None, 'a3_hits': None, 'a3_lob': None,
                'h4_runs': None, 'h4_hits': None, 'h4_lob': None, 
                'a4_runs': None, 'a4_hits': None, 'a4_lob': None,
                'h5_runs': None, 'h5_hits': None, 'h5_lob': None, 
                'a5_runs': None, 'a5_hits': None, 'a5_lob': None,
            }
            if game_season == 'Regular Season':
                # Collect stats for the first 5 innings
                for i, inning in enumerate(innings[:5], start=1):
                    stats = get_inning_stats(inning)
                    inning_stats[f'h{i}_runs'] = stats['home']['runs']
                    inning_stats[f'h{i}_hits'] = stats['home']['hits']
                    inning_stats[f'h{i}_lob'] = stats['home']['lob']
                    inning_stats[f'a{i}_runs'] = stats['away']['runs']
                    inning_stats[f'a{i}_hits'] = stats['away']['hits']
                    inning_stats[f'a{i}_lob'] = stats['away']['lob']
                try:
                    mycursor.execute('''
                        INSERT INTO firstfive (gameid, date, hometeam, awayteam, 
                                                h1_runs, h1_hits, h1_lob, 
                                                a1_runs, a1_hits, a1_lob,
                                                h2_runs, h2_hits, h2_lob, 
                                                a2_runs, a2_hits, a2_lob,
                                                h3_runs, h3_hits, h3_lob, 
                                                a3_runs, a3_hits, a3_lob,
                                                h4_runs, h4_hits, h4_lob, 
                                                a4_runs, a4_hits, a4_lob,
                                                h5_runs, h5_hits, h5_lob, 
                                                a5_runs, a5_hits, a5_lob)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        game_id, game_date_str, home_team, away_team, 
                        inning_stats['h1_runs'], inning_stats['h1_hits'], inning_stats['h1_lob'],
                        inning_stats['a1_runs'], inning_stats['a1_hits'], inning_stats['a1_lob'],
                        inning_stats['h2_runs'], inning_stats['h2_hits'], inning_stats['h2_lob'],
                        inning_stats['a2_runs'], inning_stats['a2_hits'], inning_stats['a2_lob'],
                        inning_stats['h3_runs'], inning_stats['h3_hits'], inning_stats['h3_lob'],
                        inning_stats['a3_runs'], inning_stats['a3_hits'], inning_stats['a3_lob'],
                        inning_stats['h4_runs'], inning_stats['h4_hits'], inning_stats['h4_lob'],
                        inning_stats['a4_runs'], inning_stats['a4_hits'], inning_stats['a4_lob'],
                        inning_stats['h5_runs'], inning_stats['h5_hits'], inning_stats['h5_lob'],
                        inning_stats['a5_runs'], inning_stats['a5_hits'], inning_stats['a5_lob']
                    ))
                    processedrecords += 1
                    mydb.commit()
                except mysql.connector.errors.IntegrityError as e:
                    if e.errno == 1062:  # 1062 is the error code for duplicate entry
                        print(f"Primary key violation for game_id {game_id}. Skipping this record.")
                    else:
                        raise
        mycursor.close()
        mydb.close()
        print(f"{today} Processed {processedrecords} of {possiblerecords} games into FIRSTFIVE!")


def get_inning_stats(inning):
    return {
        'home': {
            'runs': inning.get('home', {}).get('runs'),
            'hits': inning.get('home', {}).get('hits'),
            'lob': inning.get('home', {}).get('leftOnBase')
        },
        'away': {
            'runs': inning.get('away', {}).get('runs'),
            'hits': inning.get('away', {}).get('hits'),
            'lob': inning.get('away', {}).get('leftOnBase')
        }
    }

def check_rfi():
    games = yesterdays_games()
    todays_date = datetime.now() 
    if games:
        possiblerecords = len(games)
        #call get games with true because we want only the series ending games, not all games
        # Connect to the database
        mydb = mysql.connector.connect (
            host="localhost",
            user="jack",
            password="draftkings",
            database="stats"
        )       
        mycursor = mydb.cursor()
        processedrecords = 0
        for game in games:
            game_id = game['gamePk']
            away_team = game['teams']['away']['team']['name']
            away_team_id = game['teams']['away']['team']['id']
            home_team = game['teams']['home']['team']['name']
            home_team_id = game['teams']['home']['team']['id']
            game_date_str = game['officialDate']
            game_link = (game['link'])
            game_season = game['seriesDescription']

            response = requests.get(URLDOMAIN + game_link)
            data = response.json()

            # Extract relevant information
            gameinfo= data.get('gameData', {})
            scoreinfo= data.get('liveData', {})
            linescore = scoreinfo.get('linescore', {})
            winning_pitcher_id = scoreinfo.get('decisions',{}).get('winner',{}).get('id')
            winning_pitcher = scoreinfo.get('decisions',{}).get('winner',{}).get('fullName')
            losing_pitcher_id = scoreinfo.get('decisions',{}).get('loser',{}).get('id')
            losing_pitcher = scoreinfo.get('decisions',{}).get('loser',{}).get('fullName')
            innings = linescore.get('innings', {})
            first_inning = innings[0]
            home_score = first_inning.get('home',{}).get('runs')
            away_score = first_inning.get('away',{}).get('runs')


            away_pitcher = gameinfo.get('probablePitchers',{}).get('away', {}).get('fullName')
            home_pitcher = gameinfo.get('probablePitchers',{}).get('home', {}).get('fullName')
            away_pitcher_id = gameinfo.get('probablePitchers',{}).get('away', {}).get('id')
            home_pitcher_id = gameinfo.get('probablePitchers',{}).get('home', {}).get('id')
            if game_season == 'Regular Season':
                query = """
                    INSERT INTO runsfirst (gameid, date, awayteam, awaypitcher, hometeam, homepitcher, winner, loser, awayscore, homescore)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                values = (game_id, game_date_str, away_team, away_pitcher, home_team, home_pitcher, winning_pitcher, losing_pitcher, away_score, home_score)
                mycursor.execute (query, values)
                processedrecords += 1
        mydb.commit()
        mycursor.close()
        mydb.close()
        print(f"{todays_date} Processed {processedrecords} of {possiblerecords} games into RUNSFIRST!")


def yesterdays_games():
    try:
        # Calculate start and end dates
        yesterday = datetime.now() - timedelta(days=1)  # Yesterday


        # Format dates in YYYY-MM-DD
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        #################################################################################
        ####  IF YOU NEED TO RUN THIS FOR OLDER DATES, CHNAGE THE START DATE BELOW   ####
        #################################################################################
        startdate_str = yesterday_str

        # Construct URL with dynamic dates
        url = URL_BASE.format(start_date=startdate_str, end_date=yesterday_str)

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





def move2skunk():
    # Suppress the specific UserWarning from pandas
    warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")
    

    # Connect to the database
    mydb = mysql.connector.connect (
                    host="localhost",
                    user="jack",
                    password="draftkings",
                    database="stats"
    )       
    mycursor = mydb.cursor()
    todays_date = datetime.now() 

    # Set a counter to find out how many records moved
    counter = 0

    # Define the URL base for fetching MLB game data
    URL_BASE = "http://statsapi.mlb.com/api/v1/schedule/games/?sportId=1&startDate={start_date}&endDate={end_date}"

    # Query the table and load the data into a pandas DataFrame
    query = "SELECT * FROM possible_skunks"
    df = pd.read_sql(query, mydb)
    num_records = 0
    if not df.empty:
        # Convert the 'date' column to datetime format if it's not already
        df['date'] = pd.to_datetime(df['date'])

        # Find the oldest and newest date
        oldest_date = df['date'].min()
        newest_date = df['date'].max()
        num_records = len(df)

        # Format dates in YYYY-MM-DD
        start_date_str = oldest_date.strftime("%Y-%m-%d")
        end_date_str = newest_date.strftime("%Y-%m-%d")

        # Construct URL with dynamic dates
        url = URL_BASE.format(start_date=start_date_str, end_date=end_date_str)



        sql1 = "SELECT gameid, date, hometeam, homepct, homewins, awayteam, awaypct, awaywins, gamesinseries, doubleheader, hometeamseriesnum FROM possible_skunks"
        mycursor.execute(sql1)
        rows = mycursor.fetchall()
        for row in rows:
            game_id, game_date, home_team, home_pct, prior_home_wins, away_team, away_pct, prior_away_wins, games_in_series, doubleheader, hometeamseriesnum  = row 
            try:
                # Fetch game data
                response = requests.get(url)
                data = response.json()

                # Filter games
                priorgames = []
                # Don't add to prior games when missing data (example: postponed game)
                for pdate in data.get('dates', []):
                    for game in pdate.get('games', []):
                        if 'teams' in game and 'home' in game['teams'] and 'isWinner' in game['teams']['home']:
                            game_status = game['status']['statusCode']
                            
                            # Let's fix the scenario of a doubleheader as the last game in series
                            if home_team == game['teams']['home']['team']['name'] and doubleheader:
                                if hometeamseriesnum == game['teams']['home']['seriesNumber'] and game_status == "F" and game_id != game['gamePk']:
                                    if game['seriesGameNumber'] == games_in_series - 1:  #Just in case we had to go back further than one day.  We don't want first game, for example.
                                        print(f"{todays_date} Found a doubleheader lost record. Attempting to correct game count.")
                                        if game['teams']['home']['isWinner']:
                                            prior_home_wins += 1
                                        else:
                                            prior_away_wins += 1

                            if game_id == game['gamePk'] and game_status == "F":
                        
                                # Extract relevant information
                                
                                home_final_wins = game['teams']['home']['isWinner']

                                if doubleheader and prior_home_wins > 0 and prior_away_wins > 0:
                                    #if the 1st game of the doubleheader creates a scenario where the final game shouldn't
                                    #be in the skunk table, then get rid of the record.
                                    try:
                                        sql0 = f"DELETE FROM possible_skunks WHERE gameid = {game_id}"
                                        mycursor.execute(sql0)
                                        mydb.commit()
                                        print(f"{todays_date} Removed {game_id} from possible_skunks because the doubleheader invalidated the criteria.")
                                    except Exception as e:
                                        print(f"{todays_date} Error removing incorrect Doubleheader ({game_id}) from possible skunks:", e)
                                else:

                                    if home_final_wins:
                                        final_home_wins = prior_home_wins + 1
                                        final_away_wins = prior_away_wins
                                    else:
                                        final_home_wins = prior_home_wins
                                        final_away_wins = prior_away_wins +1

                                    if prior_home_wins == 0:
                                        avoiding = "home"
                                        if final_home_wins == 1:
                                            avoided = True
                                        else:
                                            avoided = False
                                    else:
                                        avoiding = "away"
                                        if final_away_wins ==1:
                                            avoided = True
                                        else:
                                            avoided = False
                                    if final_away_wins + final_home_wins == games_in_series:
                                        # MOVE THE RECORD TO THE SKUNK TABLE
            #                            print(game_id, game_status, home_final_wins, prior_home_wins, prior_away_wins, final_home_wins, final_away_wins, games_in_series, avoiding, avoided)
                                    
                                    
                                        try:
                                            sql2 = "INSERT INTO skunk_table (gameid, date, hometeam, homepct, homewins, awayteam, awaypct, awaywins, team_avoiding, skunk_avoided, gamesinseries) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                            # Tuple of values to insert
                                            values = (game_id, game_date, home_team, home_pct, final_home_wins, away_team, away_pct, final_away_wins, avoiding, avoided, games_in_series)

                                            # Execute the SQL query
                                            mycursor.execute(sql2, values)

                                            mydb.commit()
                                            counter += 1
                                        except Exception as e:
                                            print("Error writing to skunk table:", e)
                                        try:
                                            sql3 = f"DELETE FROM possible_skunks WHERE gameid = {game_id}"
                                            mycursor.execute(sql3)
                                            mydb.commit()
                                        except Exception as e:
                                            print(f"{todays_date} Error removing row from possible skunks:", e) 
                                    else:
                                        print(f"{todays_date} Game id {game_id}: Number of games is incorrect.  Possible double-header or other error.")   
            except Exception as e:
                print(f"{todays_date} Error fetching or parsing game details:", e)
    
    print(f"{todays_date} Processed {counter} of {num_records} record(s) from POSSIBLE_SKUNKS.")
    mycursor.close()
    mydb.close()

schedule.every().day.at(skunk_run_time).do(move2skunk)
schedule.every().day.at(rfi_run_time).do(check_rfi)
schedule.every().day.at(first_five_time).do(first_five)
while True:
    schedule.run_pending()
    tm.sleep(15)