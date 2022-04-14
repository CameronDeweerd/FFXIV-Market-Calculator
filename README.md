# FFXIV-Market-Calculator
This FFXIV Market Calculator helps you identify which items are profitable to purchase components for and to craft

In the MMO Final Fantasy 14 there is a marketboard where players can buy and sell items. Many of these items can also be crafted by the players.
This program uses the marketboard data and calculates which are the best items to purchase the individual components for to then craft and resell.
It looks both at the profit per item and the daily sales of a given item.

1) Pull the marketboard data using the Universalis API (https://universalis.app/docs/index.html)
2) Put the average sale price, and sale/day data into a database using SQLite
3) Using the recipe database, calculate the cost to craft and average profit
4) Output table of the top 50 most profitable items to craft (w/ optional criteria of minimum sales/day)

## Usage in Local Setup with CLI
### Prerequisites and Notes
- Python3 (Tested with >3.10)
- Pip
- To use Discord Webhook integration:
  - An existing webhook created in your discord channel
  - The Webhook ID and Webhook Token set as environment variables labelled below:
    - DISCORDID
    - DISCORDTOKEN
  - A message/messages which will be updated by the script

### Setup
1. Clone the repository using Git  
```git clone https://github.com/CameronDeweerd/FFXIV-Market-Calculator.git```
2. Change working directory into the cloned repo  
```cd ./FFXIV-Market-Calculator```
3. Using PIP install the required modules  
```pip install -r requirements.txt```
4. Edit the config.ini file referencing the Config section of this readme
5. Run the script using the command below  
```python3 main.py```

## Usage with Docker Setup
### Prerequisites and Notes
- Docker
- A folder to store persistent files in 
  - for the sake of this example I use /home/market_data
  - I also use sub-folders of this in the form of /databases and /logs
- With the below instructions you have to build the image everytime you change the config, this can be avoided if you use another volume mount to store the config file.
- For Discord Webhook integration the environment variables must be set in the DockerFile or Docker CLI arguments

### Setup
1. Clone the repository using Git  
   ```git clone https://github.com/CameronDeweerd/FFXIV-Market-Calculator.git```
2. Change working directory into the cloned repo  
   ```cd ./FFXIV-Market-Calculator```
3. Edit the config.ini file referencing the Config section of this readme
4. Build the Docker image using the current directory
   - You can replace "ffxiv_market_calc" with whatever you would like to tag the image, but it must also be replaced in later steps  
```docker build -t ffxiv_market_calc .```
5. Use the below command to run the script within the container replacing the /home/marketdata sections with whichever persistent directories you wish to use  
```docker run --rm -v /home/market_data/databases:/usr/src/app/databases -v /home/market_data/logs:/usr/src/app/logs ffxiv_market_calc```

## Config
| Option            | Values                                                    | Description                                                                                                               |
|-------------------|-----------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| MarketboardType   | `World` / `Datacenter` / `Datacentre`                     | Whether you want to pull data from a single world or all worlds in the DC                                                 |
| Datacentre        | Any FFXIV Datacentre eg. `Aether`, `Materia`, `Chaos`     | Which Datacentre you wish to pull data from, [see here for a list](https://na.finalfantasyxiv.com/lodestone/worldstatus/) |
| World             | Any FFXIV World eg. `Zalera`, `Zurvan`, `Omega`           | Which World you wish to pull data from , [see here for a list](https://na.finalfantasyxiv.com/lodestone/worldstatus/)     |
| ResultQuantity    | Any Number (Recommend 10-50)                              | How many items you wish to show in the results list                                                                       |
| UpdateQuantity    | Any Number (0 = All)                                      | How many items you wish to update from Universalis (this allows updating x results at a time for rolling updates)         |
| MinAvgSalesPerDay | Any Number (Recommend 1-20)                               | How many average sales per day an item must meet to be displayed in results                                               |
| LogEnable         | `True` / `False`                                          | Whether you want to enable logging to file                                                                                |
| LogLevel          | `CRITICAL` / `ERROR` / `WARNING` / `INFO` / `DEBUG`       | What level of logging to send to log file                                                                                 |
| LogMode           | `WRITE` / `APPEND`                                        | What filemode to use for log writing, Write = Overwrite log file each run, Append = Append to end of log file on each run |
| LogFile           | {FilePath} eg. `ffxiv_market_calc.log`                    | Filepath of the log file                                                                                                  |
| DiscordEnable     | `True` / `False`                                          | Whether or not to enable posting to Discord via Webhook (See notes on Discord in setup sections                           |
| MessageIds        | List of IDs eg. `[123456789123456789,123456789123456789]` | Used to identify the messages for the discord webhook to edit with the market data                                        |


## Example Output
![alt text](https://github.com/CameronDeweerd/FFXIV-Market-Calculator/blob/master/FFXIV%20Market.JPG?raw=true)
