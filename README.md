# FFXIV-Market-Calculator
This FFXIV Market Calculator helps you identify which items are profitable to purchase components for and to craft

In the MMO Final Fantasy 14 there is a marketboard where players can buy and sell items. Many of these items can also be crafted by the players.
This program uses the marketboard data and calculates which are the best items to purchase the individual components for to then craft and resell.
It looks both at the profit per item and the daily sales of a given item.

1) Pull the marketboard data using the Universalis API (https://universalis.app/docs/index.html)
2) Put the average sale price, and sale/day data into a database using SQLite
3) Using the recipe database, calculate the cost to craft and average profit
4) Output table of the top 50 most proftable items to craft (w/ optional criteria of minimum sales/day)
