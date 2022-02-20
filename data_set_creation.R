library(tidyverse)
library(DBI)
library(RSQLite)

wd <- dirname(sys.frame(1)$ofile) #gets the directory where this script is located
setwd(wd) #sets the working directory to the directory of this script, this is so relative paths can be used below

connection <- DBI::dbConnect(RSQLite::SQLite(), "./data/pbp_db") #create connection for db

pbp_db <- dplyr::tbl(connection, "nflfastR_pbp") #get the table from the db

#calculate the offensive yards for each team for a game
game_team_offense_yards <- pbp_db %>%
  dplyr::filter(!is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, posteam, fixed_drive) %>%
  dplyr::summarise(yards = ydsnet) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(game_id, posteam) %>%
  dplyr::summarise(off_yards = sum(yards)) %>%
  dplyr::select(game_id, posteam, off_yards)

#calculate yac total for each team for a game
game_team_yac <- pbp_db %>%
  dplyr::filter(!is.na(yards_after_catch) & !is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, posteam) %>%
  dplyr::summarise(total_yac = sum(yards_after_catch, na.rm = TRUE))

#get the teams in each game and other info like the point differential and scores of the teams
game_teams_and_info <- pbp_db %>%
  dplyr::group_by(game_id) %>%
  dplyr::summarise(home_team = home_team, away_team = away_team, home_score = home_score, away_score = away_score, result = result) %>%
  dplyr::select(game_id, home_team, away_team, home_score, away_score, result) %>% 
  dplyr::rename(point_differential = result)

#produce a result that has the info from game_teams_and_info and the offensive yards for each team and yac
combined <- game_teams_and_info %>%
  dplyr::left_join(game_team_offense_yards, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>%
  rename(home_off_yards = off_yards) %>%
  dplyr::left_join(game_team_offense_yards, by = c('game_id' = 'game_id', 'away_team' = 'posteam')) %>%
  rename(away_off_yards = off_yards) %>%
  dplyr::left_join(game_team_yac, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>%
  rename(home_yac = total_yac) %>%
  dplyr::left_join(game_team_yac, by = c('game_id' = 'game_id', 'away_team' = 'posteam')) %>%
  rename(away_yac = total_yac)

combined

DBI::dbDisconnect(connection) #disconnect from database