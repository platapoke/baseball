import discord
import mysql.connector
from discord.ext import commands
import os
import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from io import BytesIO
from columnar import columnar
# Load environment variables
from dotenv import load_dotenv
load_dotenv()

import logging

# Configure logging to ignore specific ConnectionClosed errors
logging.getLogger('discord.client').setLevel(logging.CRITICAL)
logging.getLogger('discord.gateway').setLevel(logging.CRITICAL)

TOKEN = os.getenv("DISCORD_TOKEN")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_DATABASE = os.getenv("DB_DATABASE")
SYNTAX = os.getenv("SYNTAX")
file_path = 'chart.png'

# Database connection
def get_db_connection():
    return mysql.connector.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        database=DB_DATABASE
    )

def create_pie_chart(data, title):
    # Generate a pie chart
    labels = list(data.keys())
    sizes = list(data.values())

    plt.figure(figsize=(7, 7))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title(title)
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
    plt.savefig(file_path)
    plt.close()

# SQL Execution function
def exec(query):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        return str(e)

# Function to format as table
def format_as_table(rows, headers):
    header = headers
    table = [header] + rows

    col_widths = [max(len(str(cell)) for cell in col) for col in zip(*table)]
    format_str = " | ".join([f"{{:<{width}}}" for width in col_widths])
    
    #underline header attempt
    header_line = format_str.format(*header)
    underline = " | ".join(["-" * width for width in col_widths])

    lines = [header_line, underline] + [format_str.format(*row) for row in table[1:]]
    return "\n".join(lines)

def calculate_skunk_avoidance():
    sql1 = 'SELECT (COUNT(CASE WHEN skunk_avoided = 1 THEN 1 END) / COUNT(*)) * 100 AS skunk_avoidance_percentage FROM skunk_table'
    sql1result = exec(sql1)
    avoided = sql1result[0][0]

    sql2 = 'SELECT COUNT(*) FROM skunk_table'
    sql2result = exec(sql2)
    opportunities = sql2result[0][0]
    avoided = float(avoided)
    opportunities = int(opportunities)
    skunked = 100 - avoided
    data = {'Avoided': avoided, 'Skunked': skunked}
    title = f'Skunk Percentage in {opportunities} games'
    file_path = 'chart.png'  # Overwrite the same file

    # Create the pie chart and save it as an image
    create_pie_chart(data, title)
    return avoided, skunked

def calculate_better_percent():
    sql1 = f"""SELECT 
                (SUM(CASE WHEN ((awaypct > homepct AND team_avoiding = 'away' AND skunk_avoided = 1) OR 
                                (homepct > awaypct AND team_avoiding = 'home' AND skunk_avoided = 1))
                        THEN 1 ELSE 0 END) 
                /
                SUM(CASE WHEN (awaypct > homepct AND team_avoiding = 'away') OR 
                            (homepct > awaypct AND team_avoiding = 'home') THEN 1 ELSE 0 END)) * 100 AS better_team_skunk_avoidance_percentage FROM skunk_table"""
    sql1result = exec(sql1)
    avoided = sql1result[0][0]

    sql2 = 'select count(*) from skunk_table where (awaypct > homepct and team_avoiding = "away") or (homepct > awaypct and team_avoiding = "home")'
    sql2result = exec(sql2)
    opportunities = sql2result[0][0]
    avoided = float(avoided)
    opportunities = int(opportunities)
    skunked = 100 - avoided
    data = {'Avoided': avoided, 'Skunked': skunked}
    title = f'Skunk Percentage in {opportunities} games'
    file_path = 'chart.png'  # Overwrite the same file

    # Create the pie chart and save it as an image
    create_pie_chart(data, title)
    return avoided, skunked

def calculate_home_better():
    sql1 = f"""SELECT 
                (COUNT(CASE WHEN skunk_avoided = 1 THEN 1 END) / COUNT(*)) * 100 AS htbwp
            FROM 
                skunk_table
            WHERE 
                team_avoiding = 'home' and homepct > awaypct"""
    sql1result = exec(sql1)
    avoided = sql1result[0][0]

    sql2 = 'select count(*) from skunk_table where homepct > awaypct and team_avoiding = "home"'
    sql2result = exec(sql2)
    opportunities = sql2result[0][0]
    avoided = float(avoided)
    opportunities = int(opportunities)
    skunked = 100 - avoided
    data = {'Avoided': avoided, 'Skunked': skunked}
    title = f'Skunk Percentage in {opportunities} games'
    file_path = 'chart.png'  # Overwrite the same file

    # Create the pie chart and save it as an image
    create_pie_chart(data, title)
    return avoided, skunked

def get_first_day_of_current_year():
    current_year = datetime.datetime.now().year
    first_day = datetime.date(current_year, 1, 1)
    return first_day.strftime('%Y-%m-%d')

# Function to send results as multiple embeds
async def send_sql_results(channel, embed_title, headers, sql_results):
    rows_per_embed = 30
    embed_list = []
    
    for i in range(0, len(sql_results), rows_per_embed):
        part = sql_results[i:i + rows_per_embed]
        table = format_as_table(part, headers)
        
        embed = discord.Embed(
            title=embed_title,
            description=f"```{table}```",
            color=discord.Color.blue()
        )
        embed_list.append(embed)
    
    for embed in embed_list:
        await channel.send(embed=embed)

# Discord bot setup
intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix=SYNTAX, intents=intents)

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}')
    # ? Custom Activity
    await bot.change_presence(
        activity=discord.Activity(type=discord.ActivityType.listening, name="Syntax = " + SYNTAX))

@bot.command()
async def quoteme(ctx):
    newword = ''
    for word in ctx.message.content.split():
        newword += word + " "
    replacestarter = f"{SYNTAX}quoteme "
    newword = newword.replace(replacestarter, '').strip()
    await ctx.send(
            embed=discord.Embed(title="Your quotes, sir!", description=f"'{newword}'",
                                color=discord.Color.blue()))
    return


@bot.command()
async def query(ctx):
    query = ''
    data = []
    for word in ctx.message.content.split():
        query += word + " "
    replacestarter = f"{SYNTAX}query "
    query = query.replace(replacestarter, '')

    if query == " ":
        await ctx.send(
            embed=discord.Embed(title="Error", description='Arguments are missing for the query',
                                color=discord.Color.red()))
        return

    output = exec(query)

    if not output:
        msg = await ctx.send(
            embed=discord.Embed(description="Empty list returned", color=discord.Color.blue()))
        return

    if isinstance(output, list):
        for result in output:
            sub_data = []
            for x in result:
                sub_data.append(x)
            data.append(sub_data)
        await ctx.send(columnar(data, no_borders=False))
        # await message.channel.send(output)
        return
    else:
        msg = await ctx.send(
            embed=discord.Embed(title="MySQL Returned an Error", description=output, color=discord.Color.orange()))
        await ctx.add_reaction("⚠️")
        return


@bot.command()
async def teamdata(ctx):
    first_day_of_current_year = get_first_day_of_current_year()
    query = f"""
    SELECT team_name, COUNT(*) AS attempts, SUM(CASE WHEN skunk_avoided = 1 THEN 1 ELSE 0 END) AS skunks_avoided,
    (SUM(CASE WHEN skunk_avoided = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS success_percentage
    FROM (
        SELECT CASE 
            WHEN team_avoiding = 'home' THEN hometeam 
            WHEN team_avoiding = 'away' THEN awayteam 
        END AS team_name, skunk_avoided
        FROM skunk_table
        WHERE date > '{first_day_of_current_year}'
    ) AS avoided_teams
    GROUP BY team_name
    ORDER BY attempts DESC;
    """
    
    result = exec(query)
    
    if isinstance(result, list):
        # Create a DataFrame from the result
        df = pd.DataFrame(result, columns=["Team Name", "Att", "Avoided", "% Success"])

        # Plot the DataFrame as a table and save as an image
        fig, ax = plt.subplots(figsize=(10, len(df) * 0.5))  # Adjust figsize as needed
        ax.axis('tight')
        ax.axis('off')
        table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')

        # Save the table as an image in memory
        buf = BytesIO()
        plt.savefig(buf, format='png')
        plt.close()
        buf.seek(0)

        # Create a discord file and embed it
        picture = discord.File(buf, filename='team_data.png')
        embed = discord.Embed(title="Skunk Avoidance by Team - Current Season",
                              color=discord.Color.blue())
        embed.set_image(url="attachment://team_data.png")
        
        await ctx.send(file=picture, embed=embed)
    else:
        await ctx.send(
            embed=discord.Embed(
                title="Error",
                description=result,
                color=discord.Color.red()
            )
        )











@bot.command()
async def skunktable(ctx):
    query = 'desc skunk_table'
    
    result = exec(query)
    
    if isinstance(result, list):
        header = ["Col Name", "Att", "Null", "Key"]
        await send_sql_results(ctx.channel,"Definition of skunk_table", header, result)
    else:
        await ctx.send(
            embed=discord.Embed(
                title="Error",
                description=result,
                color=discord.Color.red()
            )
        )
@bot.command()
async def percentall(ctx):
    skunk_avoidance_percentage, remaining_percentage_or_error = calculate_skunk_avoidance()

    if skunk_avoidance_percentage is not None:
        # Send the image as an attachment and embed it in the message
        with open(file_path, 'rb') as f:
            picture = discord.File(f)
            embed = discord.Embed(title="Skunk Avoidance for All Games",
                    color=discord.Color.blue())
            embed.set_image(url=f"attachment://{file_path}")
            await ctx.send(file=picture, embed=embed)
    else:
        message = remaining_percentage_or_error
    
        await ctx.send(message)

@bot.command()
async def betterpercent(ctx):
    skunk_avoidance_percentage, remaining_percentage_or_error = calculate_better_percent()

    if skunk_avoidance_percentage is not None:
        # Send the image as an attachment and embed it in the message
        with open(file_path, 'rb') as f:
            picture = discord.File(f)
            embed = discord.Embed(title="Skunk Avoidance by Better Team",
                    description="If the team with a better winning percentage was avoiding a skunk, this chart shows the percentage of games the skunk was avoided.",
                    color=discord.Color.blue())
            embed.set_image(url=f"attachment://{file_path}")
            await ctx.send(file=picture, embed=embed)
    else:
        message = remaining_percentage_or_error
    
        await ctx.send(message)

@bot.command()
async def homeandbetter(ctx):
    skunk_avoidance_percentage, remaining_percentage_or_error = calculate_home_better()

    if skunk_avoidance_percentage is not None:
        # Send the image as an attachment and embed it in the message
        with open(file_path, 'rb') as f:
            picture = discord.File(f)
            embed = discord.Embed(title="Skunk Avoidance by Home Better Team",
                    description="If the home team with a better winning percentage was avoiding a skunk, this chart shows the percentage of games the skunk was avoided.",
                    color=discord.Color.blue())
            embed.set_image(url=f"attachment://{file_path}")
            await ctx.send(file=picture, embed=embed)
    else:
        message = remaining_percentage_or_error
    
        await ctx.send(message)



@bot.command()
async def barhomeaway(ctx):
    sql1 = 'SELECT COUNT(*) FROM skunk_table'
    sql1result = exec(sql1)
    totalgames = sql1result[0][0]
    sql2 = 'SELECT COUNT(*) FROM skunk_table where skunk_avoided'
    sql2result = exec(sql2)
    avoided = sql2result[0][0]
    skunked = totalgames - avoided
    sql3 = "SELECT COUNT(*) FROM skunk_table where team_avoiding = 'home'"
    sql3result = exec(sql3)
    hometotal = sql3result[0][0]
    sql4 = "SELECT COUNT(*) FROM skunk_table where team_avoiding = 'home' and skunk_avoided"
    sql4result = exec(sql4)
    homeavoided = sql4result[0][0]
    homeskunked = hometotal - homeavoided
    sql5 = "SELECT COUNT(*) FROM skunk_table where team_avoiding = 'away'"
    sql5result = exec(sql5)
    awaytotal = sql5result[0][0]
    sql6 = "SELECT COUNT(*) FROM skunk_table where team_avoiding = 'away' and skunk_avoided"
    sql6result = exec(sql6)
    awayavoided = sql6result[0][0]
    awayskunked = awaytotal - awayavoided
    file_path='chart.png'

    categories = ['All Games', 'Home Team', 'Away Team']
    item1 = [skunked, homeskunked, awayskunked]
    item2 = [avoided, homeavoided, awayavoided]

    # Position of the bars on the x-axis
    ind = np.arange(len(categories))

            # Plotting the bars
    p1 = plt.bar(ind, item1, width=0.5, label='Skunked', color='#ff7f0e')
    p2 = plt.bar(ind, item2, width=0.5, bottom=item1, label='Avoided', color='#1f77b4')


            # Adding labels and title
    plt.ylabel('Games')

    plt.title('Skunk Avoidance - All, Home, Away')
    plt.xticks(ind, categories)
    plt.legend()

    for i in range(len(ind)):
        plt.text(ind[i], item1[i] / 2, str(item1[i]), ha='center', va='center', color='white', fontweight='bold')
        plt.text(ind[i], item1[i] + item2[i] / 2, str(item2[i]), ha='center', va='center', color='white', fontweight='bold')
    plt.savefig(file_path)
    plt.close()
    # Send the image as an attachment and embed it in the message
    with open(file_path, 'rb') as f:
        picture = discord.File(f)
        embed = discord.Embed(title="Skunk Avoidance by Home/Away team",
                    color=discord.Color.blue())
        embed.set_image(url=f"attachment://{file_path}")
        await ctx.send(file=picture, embed=embed)

@bot.command()
async def sql(ctx):
    starter = f'To perform these commands, use the prefix {SYNTAX} followed by the command name.'
    body ="""\n
        __**teamdata**__: this lists individual team skunk results for current season.\n
        __**percentall**__: pie chart showing skunk avoidance for all games in database.\n
        __**betterpercent**__: pie chart showing skunk avoidance for teams with a better winning percentage.\n
        __**homeandbetter**__: pie chart showing skunk avoidance for home teams with a better winning percentage.\n
        __**barhomeaway**__: bar graph showing relationship between home teams and away teams avoiding skunks.\n
        __**bargames**__: bar graph showing how often games avoiding are in 3 or 4 game series.\n
        __**skunktable**__: this is the table definition of skunk_table.  This is needed to form SQL statements against the database.\n
        __**query**__: use this command followed by one space and then your SQL query.  Use **skunktable** command for table definition.\n
        """
    embed = discord.Embed(title="Shortcuts for SQL commands",
                          description=starter + body,
                    color=discord.Color.blue())
    await ctx.send(embed=embed)

@bot.command()
async def bargames(ctx):
    sql1 = 'SELECT COUNT(*) FROM skunk_table'
    sql1result = exec(sql1)
    totalgames = sql1result[0][0]
    sql2 = 'SELECT COUNT(*) FROM skunk_table where skunk_avoided'
    sql2result = exec(sql2)
    avoided = sql2result[0][0]
    skunked = totalgames - avoided
    sql3 = "SELECT COUNT(*) FROM skunk_table where gamesinseries = 3"
    sql3result = exec(sql3)
    hometotal = sql3result[0][0]
    sql4 = "SELECT COUNT(*) FROM skunk_table where gamesinseries = 3 and skunk_avoided"
    sql4result = exec(sql4)
    homeavoided = sql4result[0][0]
    homeskunked = hometotal - homeavoided
    sql5 = "SELECT COUNT(*) FROM skunk_table where gamesinseries = 4"
    sql5result = exec(sql5)
    awaytotal = sql5result[0][0]
    sql6 = "SELECT COUNT(*) FROM skunk_table where gamesinseries = 4 and skunk_avoided"
    sql6result = exec(sql6)
    awayavoided = sql6result[0][0]
    awayskunked = awaytotal - awayavoided
    file_path='chart.png'

    categories = ['All Games', '3-Game Series', '4-Game Series']
    item1 = [skunked, homeskunked, awayskunked]
    item2 = [avoided, homeavoided, awayavoided]

    # Position of the bars on the x-axis
    ind = np.arange(len(categories))

            # Plotting the bars
    p1 = plt.bar(ind, item1, width=0.5, label='Skunked', color='#ff7f0e')
    p2 = plt.bar(ind, item2, width=0.5, bottom=item1, label='Avoided', color='#1f77b4')


            # Adding labels and title
    plt.ylabel('Games')

    plt.title('Skunk Avoidance - All, 3-Game, 4-Game')
    plt.xticks(ind, categories)
    plt.legend()

    for i in range(len(ind)):
        plt.text(ind[i], item1[i] / 2, str(item1[i]), ha='center', va='center', color='white', fontweight='bold')
        plt.text(ind[i], item1[i] + item2[i] / 2, str(item2[i]), ha='center', va='center', color='white', fontweight='bold')
    plt.savefig(file_path)
    plt.close()
    # Send the image as an attachment and embed it in the message
    with open(file_path, 'rb') as f:
        picture = discord.File(f)
        embed = discord.Embed(title="Skunk Avoidance by Games in Series",
                    color=discord.Color.blue())
        embed.set_image(url=f"attachment://{file_path}")
        await ctx.send(file=picture, embed=embed)


bot.run(TOKEN)
