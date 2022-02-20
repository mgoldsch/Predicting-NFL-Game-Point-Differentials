library(tidyverse)
library(DBI)
library(RSQLite)

wd <- dirname(sys.frame(1)$ofile) #gets the directory where this script is located
setwd(wd) #sets the working directory to the directory of this script, this is so relative paths can be used below

connection <- DBI::dbConnect(RSQLite::SQLite(), "./data/pbp_db") #create connection for db

pbp_db <- dplyr::tbl(connection, "nflfastR_pbp") #get the table from the db

#calculate the offensive yards for each team for a game with drive average
game_team_offense_yards <- pbp_db %>%
  dplyr::filter(!is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, posteam, fixed_drive) %>%
  dplyr::summarise(yards = ydsnet) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(game_id, posteam) %>%
  dplyr::summarise(off_yards = sum(yards, na.rm = TRUE), off_yards_drive_avg = mean(yards, na.rm = TRUE)) %>%
  dplyr::select(game_id, posteam, off_yards, off_yards_drive_avg)

#calculate the offensive yards for each team for a game (with drive avg) breakdown by quarter
game_team_offense_yards_qtr <- pbp_db %>%
  dplyr::filter(!is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, posteam, fixed_drive) %>%
  dplyr::summarise(yards = ydsnet, qtr = qtr) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(game_id, posteam, qtr) %>%
  dplyr::summarise(off_yards = sum(yards, na.rm = TRUE), off_yards_drive_avg = mean(yards, na.rm = TRUE)) %>%
  pivot_wider(names_from = qtr, values_from = c(off_yards, off_yards_drive_avg), names_glue = "{.value}_qtr_{qtr}")

#calculate the offensive passing yards for each team for a game (and drive average) (and breakdown by quarter) TODO
# game_team_offense_passing_yards <- pbp_db %>%
#   dplyr::filter(!is.na(posteam) & posteam != "") %>%
#   dplyr::group_by(game_id, posteam, fixed_drive) %>%
#   dplyr::summarise(yards = ydsnet) %>%
#   dplyr::ungroup() %>%
#   dplyr::group_by(game_id, posteam) %>%
#   dplyr::summarise(off_yards = sum(yards)) %>%
#   dplyr::select(game_id, posteam, off_yards)

#calculate the offensive rushing yards for each team for a game (and drive average) (and breakdown by quarter)TODO
# game_team_offense_yards <- pbp_db %>%
#   dplyr::filter(!is.na(posteam) & posteam != "") %>%
#   dplyr::group_by(game_id, posteam, fixed_drive) %>%
#   dplyr::summarise(yards = ydsnet) %>%
#   dplyr::ungroup() %>%
#   dplyr::group_by(game_id, posteam) %>%
#   dplyr::summarise(off_yards = sum(yards)) %>%
#   dplyr::select(game_id, posteam, off_yards)

#calculate the offensive passing yards per pass attempt for each team for a game (and breakdown by quarter) TODO

#calculate the offensive rushing yards per rush attempt for each team for a game (and breakdown by quarter) TODO

#calculate yac total and drive average for each team for a game
game_team_yac <- pbp_db %>%
  dplyr::filter(!is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, posteam, fixed_drive) %>%
  dplyr::summarise(yac_drive = sum(yards_after_catch, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(game_id, posteam) %>%
  dplyr::summarise(total_yac = sum(yac_drive, na.rm = TRUE), yac_drive_avg = sum(yac_drive, na.rm = TRUE)/n(), drive_count = n())

#calculate yac total and drive average breakdown by quarter
game_team_yac_qtr <- pbp_db %>%
  dplyr::filter(!is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, posteam, fixed_drive) %>%
  dplyr::summarise(yac_drive = sum(yards_after_catch, na.rm = TRUE), qtr = qtr) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(game_id, posteam, qtr) %>%
  dplyr::summarise(total_yac = sum(yac_drive, na.rm = TRUE), yac_drive_avg =  sum(yac_drive, na.rm = TRUE)/n(), drive_count = n()) %>%
  pivot_wider(names_from = qtr, values_from = c(total_yac, yac_drive_avg, drive_count), names_glue = "{.value}_qtr_{qtr}") %>%

#calculate the total time of possession and average time of possession of a drive for each team for a game
game_team_pos_total_avg <- pbp_db %>%
  dplyr::filter(!is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, posteam, fixed_drive) %>%
  dplyr::summarise(drive_time_of_possession = drive_time_of_possession) %>%
  dplyr::ungroup() %>%
  dplyr::collect()
game_team_pos_total_avg$drive_time_of_possession_in_sec <- lubridate::period_to_seconds(lubridate::ms(game_team_pos_total_avg$drive_time_of_possession))
game_team_pos_total_avg <- game_team_pos_total_avg %>%
  dplyr::group_by(game_id, posteam) %>%
  dplyr::summarise(total_time_pos = sum(drive_time_of_possession_in_sec, na.rm = TRUE), avg_drive_time_pos = mean(drive_time_of_possession_in_sec, na.rm = TRUE)) %>%
  dplyr::select(game_id, posteam, total_time_pos, avg_drive_time_pos)

#calculate total play count and average play count per drive for each team of a game
game_team_play_count_avg <- pbp_db %>%
  dplyr::filter(!is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, posteam, fixed_drive) %>%
  dplyr::summarise(drive_play_count = drive_play_count) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(game_id, posteam) %>%
  dplyr::summarise(total_plays = sum(drive_play_count, na.rm = TRUE), avg_plays_per_drive = mean(drive_play_count, na.rm = TRUE)) %>%
  dplyr::select(game_id, posteam, total_plays, avg_plays_per_drive)

#get total sacks and interceptions per game and per drive (defense)
game_def_sacks_ints <- pbp_db %>%
  dplyr::filter(!is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, defteam, fixed_drive) %>%
  dplyr::summarise(drive_sack_count = sum(sack, na.rm = TRUE), drive_int_count = sum(interception, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(game_id, defteam) %>%
  dplyr::summarise(total_sacks = sum(drive_sack_count, na.rm = TRUE),
                                 avg_sacks_per_drive = mean(drive_sack_count, na.rm = TRUE),
                                 total_ints = sum(drive_int_count, na.rm = TRUE),
                                 avg_ints_per_drive = mean(drive_int_count, na.rm = TRUE)) %>%
  dplyr::select(game_id, defteam, total_sacks, avg_sacks_per_drive, total_ints, avg_ints_per_drive)

#get total sacks allowed and interceptions thrown per game and per drive (offense)
game_off_sacks_ints <- pbp_db %>%
  dplyr::filter(!is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, posteam, fixed_drive) %>%
  dplyr::summarise(drive_sack_allow_count = sum(sack, na.rm = TRUE), drive_int_throw_count = sum(interception, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(game_id, posteam) %>%
  dplyr::summarise(total_sacks_allow = sum(drive_sack_allow_count, na.rm = TRUE),
                                       avg_sacks_allow_per_drive = mean(drive_sack_allow_count, na.rm = TRUE),
                                       total_ints_throw = sum(drive_int_throw_count, na.rm = TRUE),
                                       avg_ints_throw_per_drive = mean(drive_int_throw_count, na.rm = TRUE)) %>%
  dplyr::select(game_id, posteam, total_sacks_allow, avg_sacks_allow_per_drive, total_ints_throw, avg_ints_throw_per_drive)

#get the teams in each game and other info like the point differential and scores of the teams
game_teams_and_info <- pbp_db %>%
  dplyr::group_by(game_id) %>%
  dplyr::summarise(season = season, week = week, season_type = season_type, game_date,
                   start_time = start_time, roof = roof, surface = surface, weather = weather,
                   stadium = stadium, game_stadium = game_stadium,
                   home_coach = home_coach, away_coach = away_coach,
                   home_team = home_team, away_team = away_team,
                   home_score = home_score, away_score = away_score, result = result) %>%
  dplyr::select(game_id, home_team, away_team, home_score, away_score, result) %>%
  dplyr::rename(point_differential = result)

#produce a result that has the info from game_teams_and_info,the offensive yards for each team, yac, play count and average play count per drive
nfl_data_set <- game_teams_and_info %>%
  #add offensive yards
  dplyr::left_join(game_team_offense_yards, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>%
  rename(home_off_yards = off_yards, home_off_yards_drive_avg = off_yards_drive_avg) %>%
  dplyr::left_join(game_team_offense_yards, by = c('game_id' = 'game_id', 'away_team' = 'posteam')) %>%
  rename(away_off_yards = off_yards, away_off_yards_drive_avg = off_yards_drive_avg) %>%
  #add yac
  dplyr::left_join(game_team_yac, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>%
  rename(home_yac = total_yac, home_yac_drive_avg = yac_drive_avg, home_drive_count = drive_count ) %>%
  dplyr::left_join(game_team_yac, by = c('game_id' = 'game_id', 'away_team' = 'posteam')) %>%
  rename(away_yac = total_yac, away_yac_drive_avg = yac_drive_avg, away_drive_count = drive_count) %>%
  #add play count and average play count per drive
  dplyr::left_join(game_team_play_count_avg, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>%
  rename(home_total_plays = total_plays, home_plays_per_drive = avg_plays_per_drive) %>%
  dplyr::left_join(game_team_play_count_avg, by = c('game_id' = 'game_id', 'away_team' = 'posteam')) %>%
  rename(away_total_plays = total_plays, away_plays_per_drive = avg_plays_per_drive) %>%
  #TODO join def sack int
  #TODO join off sack int
  #TODO join off yards qtr breakdown
  #TODO join yac qtr breakdown
  dplyr::collect()

#write nfl_data_set to db
DBI::dbWriteTable(connection, "nfl_data_set", nfl_data_set)

#remove nfl_data_set from memory
rm(nfl_data_set)

#write game_team_pos_total_avg to db
DBI::dbWriteTable(connection, "game_team_pos_total_avg", game_team_pos_total_avg)

#remove game_team_pos_total_avg from memory
rm(game_team_pos_total_avg)

#get game_team_pos_total_avg table
game_team_pos_total_avg_sql <- dplyr::tbl(connection, "game_team_pos_total_avg") #get the game_team_pos_total_avg table from the db

#join nfl_data_set and game_team_pos_total_avg tables
nfl_data_set_sql <- dplyr::tbl(connection, "nfl_data_set") %>%  #get the nfl_data_set table from the db
  dplyr::left_join(game_team_pos_total_avg_sql, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>%
  rename(home_total_time_pos = total_time_pos, home_avg_drive_time_pos = avg_drive_time_pos) %>%
  dplyr::left_join(game_team_pos_total_avg_sql, by = c('game_id' = 'game_id', 'away_team' = 'posteam')) %>%
  rename(away_total_time_pos = total_time_pos, away_avg_drive_time_pos = avg_drive_time_pos) %>%
  dplyr::collect()

#remove nfl_data_set table from db
DBI::dbRemoveTable(connection, "nfl_data_set")

#insert nfl_data_set table in db with the joined result
DBI::dbWriteTable(connection, "nfl_data_set", nfl_data_set_sql)

#remove nfl_data_set_sql and game_team_pos_total_avg_sql from memory
rm(nfl_data_set_sql, game_team_pos_total_avg_sql)

# #delete game_team_pos_total_avg table
DBI::dbRemoveTable(connection, "game_team_pos_total_avg")

DBI::dbDisconnect(connection) #disconnect from database
