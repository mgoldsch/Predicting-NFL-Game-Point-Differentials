library(tidyverse)
library(DBI)
library(RSQLite)
library(tictoc)

tic() #start clock

#wd <- dirname(sys.frame(1)$ofile) #gets the directory where this script is located
#setwd(wd) #sets the working directory to the directory of this script, this is so relative paths can be used below

connection <- DBI::dbConnect(RSQLite::SQLite(), "./data/pbp_db") #create connection for db

pbp_db <- dplyr::tbl(connection, "nflfastR_pbp") #get the table from the db

#calculate the offensive passing yards for each team for a game (and drive average) (and breakdown by quarter)
game_team_yards <- pbp_db %>%
  dplyr::filter(!is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, posteam, fixed_drive) %>%
  dplyr::summarise(pass_yards = sum(passing_yards, na.rm = TRUE), rush_yards = sum(rushing_yards, na.rm = TRUE), qtr = qtr) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(game_id, posteam, qtr) %>%
  #summarize yards by quarter
  dplyr::summarise(off_passing_yards = sum(pass_yards, na.rm = TRUE),
                   off_rushing_yards = sum(rush_yards, na.rm = TRUE),
                   drive_count = n()) %>%
  #summarize yards drive avg by quarter
  dplyr::mutate(off_pass_yds_drive_avg = off_passing_yards/drive_count,
                off_rush_yds_drive_avg = off_rushing_yards/drive_count) %>%
  #make total_drives for use later when calculate overall drive average
  dplyr::mutate(total_drives = sum(drive_count, na.rm = TRUE)) %>%
  #make total yardage for game
  dplyr::mutate(off_pass_yards_tot = sum(off_passing_yards, na.rm = TRUE),
                off_rush_yards_tot = sum(off_rushing_yards, na.rm = TRUE)) %>%
  #make game yards drive average
  dplyr::mutate(off_pass_yards_drive_avg = off_pass_yards_tot/total_drives,
                off_rush_yards_drive_avg = off_rush_yards_tot/total_drives) %>%
  #pivot so we have a column for each metric by quarter
  pivot_wider(names_from = qtr,
              values_from = c(off_passing_yards, off_rushing_yards, off_pass_yds_drive_avg, off_rush_yds_drive_avg, drive_count),
              names_glue = "{.value}_qtr_{qtr}") %>%
  #dplyr::select(-c(total_drives, drive_count_qtr_1, drive_count_qtr_2, drive_count_qtr_3, drive_count_qtr_4)) %>%
  dplyr::compute()

#calculate the offensive passing yards per pass attempt for each team for a game (and breakdown by quarter) TODO

#calculate the offensive rushing yards per rush attempt for each team for a game (and breakdown by quarter) TODO

#calculate yac total and drive average for each team for a game
game_team_yac <- pbp_db %>%
  dplyr::filter(!is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, posteam, fixed_drive) %>%
  dplyr::summarise(yac_drive = sum(yards_after_catch, na.rm = TRUE)) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(game_id, posteam) %>%
  dplyr::summarise(total_yac = sum(yac_drive, na.rm = TRUE), yac_drive_avg = sum(yac_drive, na.rm = TRUE)/n(), drive_count = n()) %>%
  dplyr::select(-c(drive_count)) %>% 
  dplyr::compute()

#calculate yac total and drive average breakdown by quarter
game_team_yac_qtr <- pbp_db %>%
  dplyr::filter(!is.na(posteam) & posteam != "") %>%
  dplyr::group_by(game_id, posteam, fixed_drive) %>%
  dplyr::summarise(yac_drive = sum(yards_after_catch, na.rm = TRUE), qtr = qtr) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(game_id, posteam, qtr) %>%
  dplyr::summarise(total_yac = sum(yac_drive, na.rm = TRUE), yac_drive_avg =  sum(yac_drive, na.rm = TRUE)/n()) %>%
  pivot_wider(names_from = qtr, values_from = c(total_yac, yac_drive_avg), names_glue = "{.value}_qtr_{qtr}") %>%
  dplyr::compute()

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
  dplyr::select(game_id, posteam, total_plays, avg_plays_per_drive) %>%
  dplyr::compute()

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
  dplyr::select(game_id, defteam, total_sacks, avg_sacks_per_drive, total_ints, avg_ints_per_drive) %>%
  dplyr::compute()

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
  dplyr::select(game_id, posteam, total_sacks_allow, avg_sacks_allow_per_drive, total_ints_throw, avg_ints_throw_per_drive) %>%
  dplyr::compute()

#off first down stats
game_off_first_down <- pbp_db %>% 
  dplyr::filter(!is.na(posteam) & posteam != "") %>% 
  dplyr::group_by(game_id, posteam, fixed_drive) %>% 
  dplyr::summarise(qtr = qtr, drive_first_downs = drive_first_downs, rush_conv_first_down = sum(first_down_rush, na.rm = TRUE),
                   pass_conv_first_down = sum(first_down_pass, na.rm = TRUE), penalty_conv_first_down = sum(first_down_penalty, na.rm = TRUE)) %>% 
  dplyr::ungroup() %>% 
  dplyr::group_by(game_id, posteam, qtr) %>% 
  dplyr::summarise(first_downs = sum(drive_first_downs, na.rm = TRUE), rush_conversions_first_down = sum(rush_conv_first_down, na.rm = TRUE), 
                   pass_conversions_first_down = sum(pass_conv_first_down, na.rm = TRUE), penalty_conversions_first_down = sum(penalty_conv_first_down, na.rm = TRUE), 
                   drive_count = n()) %>% 
  dplyr::mutate(first_down_drive_avg = first_downs/drive_count,
                rush_conv_first_down_drive_avg = rush_conversions_first_down/drive_count, 
                pass_conv_first_down_drive_avg = pass_conversions_first_down/drive_count, 
                penalty_conv_first_down_drive_avg = penalty_conversions_first_down/drive_count) %>% 
  dplyr::mutate(total_drives = sum(drive_count, na.rm = TRUE)) %>% 
  dplyr::mutate(first_down_tot = sum(first_downs, na.rm = TRUE), 
                rush_conv_first_down_tot = sum(rush_conversions_first_down, na.rm = TRUE), 
                pass_conv_first_down_tot = sum(pass_conversions_first_down, na.rm = TRUE), 
                penalty_conv_first_down_tot = sum(penalty_conversions_first_down, na.rm = TRUE)) %>%
  dplyr::mutate(first_down_tot_drive_avg = first_down_tot/total_drives,
                rush_conv_first_down_tot_drive_avg = rush_conv_first_down_tot/total_drives, 
                pass_conv_first_down_tot_drive_avg = pass_conv_first_down_tot/total_drives, 
                penalty_conv_first_down_tot_drive_avg = penalty_conv_first_down_tot/total_drives) %>% 
  dplyr::select(-c(drive_count)) %>% 
  pivot_wider(names_from = qtr,
              values_from = c(first_downs, rush_conversions_first_down, pass_conversions_first_down, penalty_conversions_first_down, 
                              first_down_drive_avg, rush_conv_first_down_drive_avg, pass_conv_first_down_drive_avg, penalty_conv_first_down_drive_avg),
              names_glue = "{.value}_qtr_{qtr}") %>%
  dplyr::select(-c(total_drives)) %>%
  dplyr::compute()

#add epa
# game_off_epa <- pbp_db %>% 
#   dplyr::filter(!is.na(posteam) & posteam != "") %>% 
#   dplyr::group_by(game_id, posteam, fixed_drive) %>% 
#   dplyr::summarise(qtr = qtr, drive_first_downs = drive_first_downs, rush_conv_first_down = sum(first_down_rush, na.rm = TRUE),
#                    pass_conv_first_down = sum(first_down_pass, na.rm = TRUE), penalty_conv_first_down = sum(first_down_penalty, na.rm = TRUE)) %>% 
#   dplyr::ungroup() %>% 
#   dplyr::group_by(game_id, posteam, qtr) %>% 
#   dplyr::summarise(first_downs = sum(drive_first_downs, na.rm = TRUE), rush_conversions_first_down = sum(rush_conv_first_down, na.rm = TRUE), 
#                    pass_conversions_first_down = sum(pass_conv_first_down, na.rm = TRUE), penalty_conversions_first_down = sum(penalty_conv_first_down, na.rm = TRUE), 
#                    drive_count = n()) %>% 
#   dplyr::mutate(first_down_drive_avg = first_downs/drive_count,
#                 rush_conv_first_down_drive_avg = rush_conversions_first_down/drive_count, 
#                 pass_conv_first_down_drive_avg = pass_conversions_first_down/drive_count, 
#                 penalty_conv_first_down_drive_avg = penalty_conversions_first_down/drive_count) %>% 
#   dplyr::mutate(total_drives = sum(drive_count, na.rm = TRUE)) %>% 
#   dplyr::mutate(first_down_tot = sum(first_downs, na.rm = TRUE), 
#                 rush_conv_first_down_tot = sum(rush_conversions_first_down, na.rm = TRUE), 
#                 pass_conv_first_down_tot = sum(pass_conversions_first_down, na.rm = TRUE), 
#                 penalty_conv_first_down_tot = sum(penalty_conversions_first_down, na.rm = TRUE)) %>%
#   dplyr::mutate(first_down_tot_drive_avg = first_down_tot/total_drives,
#                 rush_conv_first_down_tot_drive_avg = rush_conv_first_down_tot/total_drives, 
#                 pass_conv_first_down_tot_drive_avg = pass_conv_first_down_tot/total_drives, 
#                 penalty_conv_first_down_tot_drive_avg = penalty_conv_first_down_tot/total_drives) %>% 
#   dplyr::select(-c(drive_count)) %>% 
#   pivot_wider(names_from = qtr,
#               values_from = c(first_downs, rush_conversions_first_down, pass_conversions_first_down, penalty_conversions_first_down, 
#                               first_down_drive_avg, rush_conv_first_down_drive_avg, pass_conv_first_down_drive_avg, penalty_conv_first_down_drive_avg),
#               names_glue = "{.value}_qtr_{qtr}") %>%
#   dplyr::select(-c(total_drives)) %>%
#   dplyr::compute()
#add wpa


#get the teams in each game and other info like the point differential and scores of the teams
game_teams_and_info <- pbp_db %>%
  dplyr::group_by(game_id) %>%
  dplyr::summarise(season = season, week = week, season_type = season_type, game_date = game_date,
                   start_time = start_time, roof = roof, surface = surface, weather = weather, game_stadium = game_stadium,
                   home_coach = home_coach, away_coach = away_coach,
                   home_team = home_team, away_team = away_team,
                   home_score = home_score, away_score = away_score, result = result, spread_line = spread_line) %>%
  dplyr::select(game_id, season, week, season_type, game_date, start_time, roof, surface, weather, game_stadium, 
                home_team, away_team, home_score, away_score, result, home_coach, away_coach, spread_line) %>%
  dplyr::rename(point_differential = result) %>%
  dplyr::compute()

#produce a result that has the info from game_teams_and_info,the offensive yards for each team, yac, play count and average play count per drive
nfl_data_set <- game_teams_and_info %>%
  #add offensive yards
  dplyr::left_join(game_team_yards, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>%
  dplyr::left_join(game_team_yards, by = c('game_id' = 'game_id', 'away_team' = 'posteam'), suffix = c("_home", "_away")) %>%
  #add defensive allowed yards
  dplyr::left_join(game_team_yards, by = c('game_id' = 'game_id', 'away_team' = 'posteam')) %>%
  dplyr::left_join(game_team_yards, by = c('game_id' = 'game_id', 'home_team' = 'posteam'), suffix = c("_home_defense_allowed", "_away_defense_allowed")) %>%
  #add yac
  dplyr::left_join(game_team_yac, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>%
  rename(home_yac = total_yac, home_yac_drive_avg = yac_drive_avg) %>%
  dplyr::left_join(game_team_yac, by = c('game_id' = 'game_id', 'away_team' = 'posteam')) %>%
  rename(away_yac = total_yac, away_yac_drive_avg = yac_drive_avg) %>%
  #compute to avoid parser stack overflow error
  dplyr::compute() %>%
  #add yac qtr
  dplyr::left_join(game_team_yac_qtr, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>% 
  dplyr::left_join(game_team_yac_qtr, by = c('game_id' = 'game_id', 'away_team' = 'posteam'), suffix = c("_home", "_away")) %>%
  #add play count and average play count per drive
  dplyr::left_join(game_team_play_count_avg, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>%
  rename(home_total_plays = total_plays, home_plays_per_drive = avg_plays_per_drive) %>%
  dplyr::left_join(game_team_play_count_avg, by = c('game_id' = 'game_id', 'away_team' = 'posteam')) %>%
  rename(away_total_plays = total_plays, away_plays_per_drive = avg_plays_per_drive) %>%
  #join def sack int
  dplyr::left_join(game_def_sacks_ints, by = c('game_id' = 'game_id', 'home_team' = 'defteam')) %>%
  dplyr::left_join(game_def_sacks_ints, by = c('game_id' = 'game_id', 'away_team' = 'defteam'), suffix = c('_home', '_away')) %>%
  #join off sack int
  dplyr::left_join(game_off_sacks_ints, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>%
  dplyr::left_join(game_off_sacks_ints, by = c('game_id' = 'game_id', 'away_team' = 'posteam'), suffix = c('_home', '_away')) %>%
  #join first downs
  dplyr::left_join(game_off_first_down, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>% 
  dplyr::left_join(game_off_first_down, by = c('game_id' = 'game_id', 'away_team' = 'posteam'), suffix = c('_home', '_away')) %>%
  #join first downs defense allowed
  dplyr::left_join(game_off_first_down, by = c('game_id' = 'game_id', 'home_team' = 'posteam')) %>% 
  dplyr::left_join(game_off_first_down, by = c('game_id' = 'game_id', 'away_team' = 'posteam'), suffix = c('_home_defense_allowed', '_away_defense_allowed')) %>%
  #collect
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

#remove nfl_data_set_sql
rm(nfl_data_set_sql)

#delete game_team_pos_total_avg table
DBI::dbRemoveTable(connection, "game_team_pos_total_avg")

#elo
teams <- dplyr::tbl(connection, "nfl_data_set") %>% #get list of teams
  dplyr::distinct(home_team) %>% 
  dplyr::pull()
elo <- rep(1500, length(teams))

teams <- cbind.data.frame(teams, elo)
names(teams) <- c("teams", "elo")

game_count <- dplyr::tbl(connection, "nfl_data_set") %>% 
  dplyr::summarise(game_count = n()) %>% 
  dplyr::select(game_count) %>% 
  dplyr::pull()

games <- dplyr::tbl(connection, "nfl_data_set") %>% 
  dplyr::select(game_id, season, week, home_team, away_team, point_differential) %>%
  dplyr::collect()

elo_vec_1 <- elo_vec_2 <- rep(NA, game_count)

# For each game in our dataset
for(i in 1:game_count){
  if(i > 2){
    if(games$season[i] != games$season[i-1]){
      teams$elo <- 1500
    }
  }
  
  # Extract team 1 Elo
  team1_elo <- teams$elo[teams$team == games$home_team[i]]
  # Extract team 2 Elo
  team2_elo <- teams$elo[teams$team ==  games$away_team[i]]
  
  # Store elo values
  elo_vec_1[i] <- team1_elo
  elo_vec_2[i] <- team2_elo
  
  # Calculate new Elo ratings
  new_elo <- elo.calc(wins.A = ifelse(games$point_differential[i] > 0, 1, ifelse(games$point_differential[i] == 0, .5, 0)),
                      elo.A = team1_elo,
                      elo.B = team2_elo,
                      k= 100) #update rate
  
  # Store new elo ratings for home team
  teams$elo[teams$team ==games$home_team[i]] <- new_elo[1,1]
  # Store new elo ratings for away team
  teams$elo[teams$team == games$away_team[i]] <- new_elo[1,2]
}

#create elo dataframe
elo_df <- cbind.data.frame(games, home_elo = elo_vec_1, away_elo = elo_vec_2)
elo_df <- elo_df[, c("game_id", "home_elo", "away_elo")]

#write elo_df to db
DBI::dbWriteTable(connection, "elo_df", elo_df)

#remove rollmean_df from memory
rm(elo_df)

#get elo_df table
elo_df_sql <- dplyr::tbl(connection, "elo_df") #get the elo_df table from the db

#join nfl_data_set and elo_df tables
nfl_data_set_sql <- dplyr::tbl(connection, "nfl_data_set") %>%  #get the nfl_data_set table from the db
  dplyr::left_join(elo_df_sql, by = c('game_id' = 'game_id')) %>%
  dplyr::collect()

#remove nfl_data_set table from db
DBI::dbRemoveTable(connection, "nfl_data_set")

#insert nfl_data_set table in db with the joined result
DBI::dbWriteTable(connection, "nfl_data_set", nfl_data_set_sql)

#remove nfl_data_set_sql from memory
rm(nfl_data_set_sql)

#delete rollmean_df table
DBI::dbRemoveTable(connection, "elo_df")


#create rollmeans
library(zoo) #for rollmean
library(roll) #for weighted rollmean

teams <- dplyr::tbl(connection, "nfl_data_set") %>% #get list of teams
  dplyr::distinct(home_team) %>% 
  dplyr::pull()

nfl_rollmeans <- dplyr::tbl(connection, "nfl_data_set") %>% #get the nfl_data_set table from the db
  #dplyr::select(game_id, home_team, away_team, home_score, away_score, point_differential, 
   #             off_pass_yards_tot_home, off_rush_yards_tot_home, off_pass_yards_tot_away, off_rush_yards_tot_away) %>% #select columns to consider
  dplyr::collect()

#variables that are going to have rollmean applied
met_h <- c("home_score", "off_pass_yards_tot_home", "off_rush_yards_tot_home", 
           "off_passing_yards_qtr_1_home", "off_passing_yards_qtr_2_home", 
           "off_passing_yards_qtr_3_home", "off_passing_yards_qtr_4_home", 
           "off_rushing_yards_qtr_1_home", "off_rushing_yards_qtr_2_home", 
           "off_rushing_yards_qtr_3_home", "off_rushing_yards_qtr_4_home", 
           "home_yac", "total_yac_qtr_1_home", "total_yac_qtr_2_home", "total_yac_qtr_3_home", "total_yac_qtr_4_home", 
           "home_total_plays", "total_sacks_home", "total_ints_home", "total_sacks_allow_home", "total_ints_throw_home", "home_total_time_pos", 
           "total_drives_home", "drive_count_qtr_1_home", "drive_count_qtr_2_home", "drive_count_qtr_3_home", "drive_count_qtr_4_home") #vector of variables to apply rollmeans to (home varaibles)
met_h <- c(met_h, sapply(c(met_h[2:11], met_h[23:27]), function(x){paste0(x, "_defense_allowed")}, USE.NAMES = FALSE)) #create defense allowed variable names
met_h <- c(met_h, sapply(c("first_down_tot", "rush_conv_first_down_tot", 
           "pass_conv_first_down_tot", "penalty_conv_first_down_tot", "first_downs_qtr_1", 
           "first_downs_qtr_2", "first_downs_qtr_3", "first_downs_qtr_4", "rush_conversions_first_down_qtr_1", "rush_conversions_first_down_qtr_2", 
           "rush_conversions_first_down_qtr_3", "rush_conversions_first_down_qtr_4", "pass_conversions_first_down_qtr_1", 
           "pass_conversions_first_down_qtr_2", "pass_conversions_first_down_qtr_3", 
           "pass_conversions_first_down_qtr_4", "penalty_conversions_first_down_qtr_1", "penalty_conversions_first_down_qtr_2", 
           "penalty_conversions_first_down_qtr_3", "penalty_conversions_first_down_qtr_4"), function(x){paste0(x, "_home")}, USE.NAMES = FALSE))
met_h <- c(met_h, sapply(c("first_down_tot", "rush_conv_first_down_tot", 
                           "pass_conv_first_down_tot", "penalty_conv_first_down_tot", "first_downs_qtr_1", 
                           "first_downs_qtr_2", "first_downs_qtr_3", "first_downs_qtr_4", "rush_conversions_first_down_qtr_1", "rush_conversions_first_down_qtr_2", 
                           "rush_conversions_first_down_qtr_3", "rush_conversions_first_down_qtr_4", "pass_conversions_first_down_qtr_1", 
                           "pass_conversions_first_down_qtr_2", "pass_conversions_first_down_qtr_3", 
                           "pass_conversions_first_down_qtr_4", "penalty_conversions_first_down_qtr_1", "penalty_conversions_first_down_qtr_2", 
                           "penalty_conversions_first_down_qtr_3", "penalty_conversions_first_down_qtr_4"), function(x){paste0(x, "_home_defense_allowed")}, USE.NAMES = FALSE))

met_a <- c("away_score", "off_pass_yards_tot_away", "off_rush_yards_tot_away", 
           "off_passing_yards_qtr_1_away", "off_passing_yards_qtr_2_away", 
           "off_passing_yards_qtr_3_away", "off_passing_yards_qtr_4_away", 
           "off_rushing_yards_qtr_1_away", "off_rushing_yards_qtr_2_away", 
           "off_rushing_yards_qtr_3_away", "off_rushing_yards_qtr_4_away", 
           "away_yac", "total_yac_qtr_1_away", "total_yac_qtr_2_away", "total_yac_qtr_3_away", "total_yac_qtr_4_away", 
           "away_total_plays", "total_sacks_away", "total_ints_away", "total_sacks_allow_away", "total_ints_throw_away", "away_total_time_pos", 
           "total_drives_away", "drive_count_qtr_1_away", "drive_count_qtr_2_away", "drive_count_qtr_3_away", "drive_count_qtr_4_away") #vector of variables to apply rollmeans to (away varaibles)
met_a <- c(met_a, sapply(c(met_a[2:11], met_a[23:27]), function(x){paste0(x, "_defense_allowed")}, USE.NAMES = FALSE)) #create defense allowed variable names
met_a <- c(met_a, sapply(c("first_down_tot", "rush_conv_first_down_tot", 
                           "pass_conv_first_down_tot", "penalty_conv_first_down_tot", "first_downs_qtr_1", 
                           "first_downs_qtr_2", "first_downs_qtr_3", "first_downs_qtr_4", "rush_conversions_first_down_qtr_1", "rush_conversions_first_down_qtr_2", 
                           "rush_conversions_first_down_qtr_3", "rush_conversions_first_down_qtr_4", "pass_conversions_first_down_qtr_1", 
                           "pass_conversions_first_down_qtr_2", "pass_conversions_first_down_qtr_3", 
                           "pass_conversions_first_down_qtr_4", "penalty_conversions_first_down_qtr_1", "penalty_conversions_first_down_qtr_2", 
                           "penalty_conversions_first_down_qtr_3", "penalty_conversions_first_down_qtr_4"), function(x){paste0(x, "_away")}, USE.NAMES = FALSE))
met_a <- c(met_a, sapply(c("first_down_tot", "rush_conv_first_down_tot", 
                           "pass_conv_first_down_tot", "penalty_conv_first_down_tot", "first_downs_qtr_1", 
                           "first_downs_qtr_2", "first_downs_qtr_3", "first_downs_qtr_4", "rush_conversions_first_down_qtr_1", "rush_conversions_first_down_qtr_2", 
                           "rush_conversions_first_down_qtr_3", "rush_conversions_first_down_qtr_4", "pass_conversions_first_down_qtr_1", 
                           "pass_conversions_first_down_qtr_2", "pass_conversions_first_down_qtr_3", 
                           "pass_conversions_first_down_qtr_4", "penalty_conversions_first_down_qtr_1", "penalty_conversions_first_down_qtr_2", 
                           "penalty_conversions_first_down_qtr_3", "penalty_conversions_first_down_qtr_4"), function(x){paste0(x, "_away_defense_allowed")}, USE.NAMES = FALSE))


#rollmean variables that are already averages (need to apply weighted rolling averages)
avg_metrics <- c("off_pass_yards_drive_avg", "off_rush_yards_drive_avg", "off_pass_yds_drive_avg_qtr_1",
                 "off_pass_yds_drive_avg_qtr_2", "off_pass_yds_drive_avg_qtr_3", "off_pass_yds_drive_avg_qtr_4",
                 "off_rush_yds_drive_avg_qtr_1", "off_rush_yds_drive_avg_qtr_2", "off_rush_yds_drive_avg_qtr_3", "off_rush_yds_drive_avg_qtr_4",
                 "yac_drive_avg_qtr_1", "yac_drive_avg_qtr_2", "yac_drive_avg_qtr_3", "yac_drive_avg_qtr_4",
                 "avg_sacks_allow_per_drive", "avg_ints_throw_per_drive", "avg_sacks_per_drive", "avg_ints_per_drive")
avg_metrics <- c(avg_metrics, "first_down_tot_drive_avg", "rush_conv_first_down_tot_drive_avg", "pass_conv_first_down_tot_drive_avg", 
                 "penalty_conv_first_down_tot_drive_avg", "first_down_drive_avg_qtr_1", "first_down_drive_avg_qtr_2", "first_down_drive_avg_qtr_3", 
                 "first_down_drive_avg_qtr_4", "rush_conv_first_down_drive_avg_qtr_1", "rush_conv_first_down_drive_avg_qtr_2", "rush_conv_first_down_drive_avg_qtr_3", 
                 "rush_conv_first_down_drive_avg_qtr_4", "pass_conv_first_down_drive_avg_qtr_1", "pass_conv_first_down_drive_avg_qtr_2", "pass_conv_first_down_drive_avg_qtr_3", "pass_conv_first_down_drive_avg_qtr_4", "penalty_conv_first_down_drive_avg_qtr_1", 
                 "penalty_conv_first_down_drive_avg_qtr_2", "penalty_conv_first_down_drive_avg_qtr_3", "penalty_conv_first_down_drive_avg_qtr_4")

avg_met_weights <- c(rep("total_drives", 2), rep(c("drive_count_qtr_1", "drive_count_qtr_2", "drive_count_qtr_3", "drive_count_qtr_4"), 3), 
                     rep("total_drives", 6), rep(c("drive_count_qtr_1", "drive_count_qtr_2", "drive_count_qtr_3", "drive_count_qtr_4"), 4))

avg_metrics_h <- c(sapply(avg_metrics, function(x){paste0(x, "_home")}, USE.NAMES = FALSE), "home_yac_drive_avg", "home_plays_per_drive", "home_avg_drive_time_pos")
avg_metrics_h<- c(avg_metrics_h, sapply(c(avg_metrics_h[1:10], avg_metrics_h[19:38]), function(x){paste0(x, "_defense_allowed")}, USE.NAMES = FALSE)) #create defense allowed variable names

avg_met_weights_h <- c(sapply(avg_met_weights, function(x){paste0(x, "_home")}, USE.NAMES = FALSE), 
                       rep("total_drives_home_defense_allowed", 2), rep("total_drives_home", 3),
                       rep("total_drives_home_defense_allowed", 2), 
                       rep(c("drive_count_qtr_1_home_defense_allowed", "drive_count_qtr_2_home_defense_allowed", 
                             "drive_count_qtr_3_home_defense_allowed", "drive_count_qtr_4_home_defense_allowed"), 2), 
                       rep("total_drives_home_defense_allowed", 4), 
                       rep(c("drive_count_qtr_1_home_defense_allowed", "drive_count_qtr_2_home_defense_allowed", "drive_count_qtr_3_home_defense_allowed", "drive_count_qtr_4_home_defense_allowed"), 4))

avg_metrics_a <- c(sapply(avg_metrics, function(x){paste0(x, "_away")}, USE.NAMES = FALSE), "away_yac_drive_avg", "away_plays_per_drive", "away_avg_drive_time_pos")
avg_metrics_a<- c(avg_metrics_a, sapply(c(avg_metrics_a[1:10], avg_metrics_a[19:38]), function(x){paste0(x, "_defense_allowed")}, USE.NAMES = FALSE)) #create defense allowed variable names

avg_met_weights_a <- c(sapply(avg_met_weights, function(x){paste0(x, "_away")}, USE.NAMES = FALSE),
                       rep("total_drives_away_defense_allowed", 2), rep("total_drives_away", 3), 
                       rep("total_drives_away_defense_allowed", 2), 
                       rep(c("drive_count_qtr_1_away_defense_allowed", "drive_count_qtr_2_away_defense_allowed", 
                             "drive_count_qtr_3_away_defense_allowed", "drive_count_qtr_4_away_defense_allowed"), 2), 
                       rep("total_drives_away_defense_allowed", 4), 
                       rep(c("drive_count_qtr_1_away_defense_allowed", "drive_count_qtr_2_away_defense_allowed", "drive_count_qtr_3_away_defense_allowed", "drive_count_qtr_4_away_defense_allowed"), 4))

rm_names_met_h_list <- list()
rm_names_met_a_list <- list()

rm_names_avg_met_h_list <- list()
rm_names_avg_met_a_list <- list()

wl_list <- c(5,3)
#wl = 5 #setting week lap to 5
#add 3 week average as well
#ratio between 3 week and 5 week averages

q <- 0 #week loop counter

for(wl in wl_list){
  q <- q + 1 #week loop +1
  
  rm_names_met_h <- sapply(met_h, function(x){paste0(x, "_", wl, "w_avg")}, USE.NAMES = FALSE) #vector of names the new variables will be (home variables)
  rm_names_met_a <- sapply(met_a, function(x){paste0(x, "_", wl, "w_avg")}, USE.NAMES = FALSE) #vector of names the new variables will be (away variables)
  
  rm_names_point_diff <- c(paste0("home_point_differential_", wl, "w_avg"), paste0("away_point_differential_", wl, "w_avg")) #vector of names for the new point_differential variables
  
  rm_names_avg_met_h <- sapply(avg_metrics_h, function(x){paste0(x, "_", wl, "w_avg")}, USE.NAMES = FALSE) #vector of names the avg new variables will be (home variables)
  rm_names_avg_met_a <- sapply(avg_metrics_a, function(x){paste0(x, "_", wl, "w_avg")}, USE.NAMES = FALSE) #vector of names the avg new variables will be (away variables)
  
  #add variable names to lists
  rm_names_met_h_list[[q]] <- rm_names_met_h
  rm_names_met_a_list[[q]] <- rm_names_met_a
  
  rm_names_avg_met_h_list[[q]] <- rm_names_avg_met_h
  rm_names_avg_met_a_list[[q]] <- rm_names_avg_met_a
  
  home_rollmean_df_list <- list() #used to store dfs from loop where the team is the home team
  away_rollmean_df_list <- list() #used to store dfs from loop where the team is the away team
  
  i <- 0 #team loop counter
  
  #calculate rollmean variables for each team
  for(t in teams){ #loop through each team
    i <- i + 1 #add 1 to loop counter
    
    rollmean_temp <- as.data.frame(nfl_rollmeans) #convert to dataframe
    rollmean_temp <- rollmean_temp[rollmean_temp$home_team == t | rollmean_temp$away_team == t, ] #get only games of 1 team
    
    for (j in 1:length(met_h)){ #loop through each metric we are applying rollmean to
      rm_values <- zoo::rollapply(ifelse(rollmean_temp$home_team == t, rollmean_temp[, met_h[j]], rollmean_temp[,met_a[j]]), list(-seq(wl)), mean,  fill = NA) #calculate rollmean of metric for team
      rollmean_temp[, rm_names_met_h[j]] <- ifelse(rollmean_temp$home_team == t, rm_values, NA) #populate the rollmean value into home column if the team is home
      rollmean_temp[, rm_names_met_a[j]] <- ifelse(rollmean_temp$away_team == t, rm_values, NA) #populate the rollmean value into away column if the team is away
    }
    
    #rollmean point_differential
    point_diff_avg <- zoo::rollapply(ifelse(rollmean_temp$home_team == t, rollmean_temp$point_differential, rollmean_temp$point_differential * -1), list(-seq(wl)), mean, fill = NA)
    rollmean_temp[, rm_names_point_diff[1]] <- ifelse(rollmean_temp$home_team == t, point_diff_avg, NA) #populate the rollmean point_diff_avg into home column if the team is home
    rollmean_temp[, rm_names_point_diff[2]] <- ifelse(rollmean_temp$away_team == t, point_diff_avg, NA) #populate the rollmean point_diff_avg into away column if the team is away
    
    for (k in 1:length(avg_metrics_h)){ #loop through each avg metric we are applying rollmean to
      rm_values_avg <- Hmisc::Lag(roll::roll_mean(ifelse(rollmean_temp$home_team == t, rollmean_temp[, avg_metrics_h[k]], rollmean_temp[, avg_metrics_a[k]]), 
                                                  width = wl,
                                                  weights = ifelse(rollmean_temp$home_team == t, rollmean_temp[, avg_met_weights_h[k]], rollmean_temp[, avg_met_weights_a[k]]), 
                                                  online = FALSE), 1) #calculate rollmean of metric for teams
      rollmean_temp[, rm_names_avg_met_h[k]] <- ifelse(rollmean_temp$home_team == t, rm_values_avg, NA) #populate the rollmean value into home column if the team is home
      rollmean_temp[, rm_names_avg_met_a[k]] <- ifelse(rollmean_temp$away_team == t, rm_values_avg, NA) #populate the rollmean value into away column if the team is away
    }
    
    #separate into home and away
    home_rollmean_temp_df <- rollmean_temp[rollmean_temp$home_team == t, which(names(rollmean_temp) %in% c("game_id", rm_names_met_h, rm_names_point_diff[1], rm_names_avg_met_h))] #create df of only games where the team was the home team and include only game_id and rollmean home columns
    away_rollmean_temp_df <- rollmean_temp[rollmean_temp$away_team == t, which(names(rollmean_temp) %in% c("game_id", rm_names_met_a, rm_names_point_diff[2], rm_names_avg_met_a))] #create df of only games where the team was the away team and include only game_id and rollmean away columns
    
    home_rollmean_df_list[[i]] <- home_rollmean_temp_df #add df to list
    away_rollmean_df_list[[i]] <- away_rollmean_temp_df #add df to list
  }
  
  #combine dfs in lists
  home_rollmean_df <- do.call(rbind, home_rollmean_df_list)
  away_rollmean_df <- do.call(rbind, away_rollmean_df_list)
  
  rollmean_df <- merge(home_rollmean_df, away_rollmean_df, by = "game_id") #merge home and away dfs
  
  #write rollmean_df to db
  DBI::dbWriteTable(connection, "rollmean_df", rollmean_df)
  
  #remove rollmean_df from memory
  rm(rollmean_df)
  
  #get rollmean_df table
  rollmean_df_sql <- dplyr::tbl(connection, "rollmean_df") #get the rollmean_df table from the db
  
  #join nfl_data_set and rollmean_df tables
  nfl_data_set_sql <- dplyr::tbl(connection, "nfl_data_set") %>%  #get the nfl_data_set table from the db
    dplyr::left_join(rollmean_df_sql, by = c('game_id' = 'game_id')) %>%
    dplyr::collect()
  
  #remove nfl_data_set table from db
  DBI::dbRemoveTable(connection, "nfl_data_set")
  
  #insert nfl_data_set table in db with the joined result
  DBI::dbWriteTable(connection, "nfl_data_set", nfl_data_set_sql)
  
  #remove nfl_data_set_sql from memory
  rm(nfl_data_set_sql)
  
  #delete rollmean_df table
  DBI::dbRemoveTable(connection, "rollmean_df")
}

#create rollmean variables ratio variables
rollmean_ratio_df <- as.data.frame(dplyr::tbl(connection, "nfl_data_set") %>% dplyr::collect())

rollmean_ratio_var_names_met_h <- mapply(function(x, y){paste0(x, "_RATIO_", y)}, rm_names_met_h_list[2], rm_names_met_h_list[1])
for(i in 1:length(rollmean_ratio_var_names_met_h)){
  rollmean_ratio_df[, rollmean_ratio_var_names_met_h[i]] <- ifelse(rollmean_ratio_df[, rm_names_met_h_list[[1]][i]] == 0, NA, rollmean_ratio_df[, rm_names_met_h_list[[2]][i]]/rollmean_ratio_df[, rm_names_met_h_list[[1]][i]])
}

rollmean_ratio_var_names_met_a <- mapply(function(x, y){paste0(x, "_RATIO_", y)}, rm_names_met_a_list[2], rm_names_met_a_list[1])
for(i in 1:length(rollmean_ratio_var_names_met_a)){
  rollmean_ratio_df[, rollmean_ratio_var_names_met_a[i]] <- ifelse(rollmean_ratio_df[, rm_names_met_a_list[[1]][i]] == 0, NA, rollmean_ratio_df[, rm_names_met_a_list[[2]][i]]/rollmean_ratio_df[, rm_names_met_a_list[[1]][i]])
}

rollmean_ratio_var_names_met_avg_h <- mapply(function(x, y){paste0(x, "_RATIO_", y)}, rm_names_avg_met_h_list[2], rm_names_avg_met_h_list[1])
for(i in 1:length(rollmean_ratio_var_names_met_avg_h)){
  rollmean_ratio_df[, rollmean_ratio_var_names_met_avg_h[i]] <- ifelse(rollmean_ratio_df[, rm_names_avg_met_h_list[[1]][i]] == 0, NA, rollmean_ratio_df[, rm_names_avg_met_h_list[[2]][i]]/rollmean_ratio_df[, rm_names_avg_met_h_list[[1]][i]])
}

rollmean_ratio_var_names_met_avg_a <- mapply(function(x, y){paste0(x, "_RATIO_", y)}, rm_names_avg_met_a_list[2], rm_names_avg_met_a_list[1])
for(i in 1:length(rollmean_ratio_var_names_met_avg_a)){
  rollmean_ratio_df[, rollmean_ratio_var_names_met_avg_a[i]] <- ifelse(rollmean_ratio_df[, rm_names_avg_met_a_list[[1]][i]] == 0, NA, rollmean_ratio_df[, rm_names_avg_met_a_list[[2]][i]]/rollmean_ratio_df[, rm_names_avg_met_a_list[[1]][i]])
}

rollmean_ratio_df <- rollmean_ratio_df[, which(names(rollmean_ratio_df) %in% c("game_id", rollmean_ratio_var_names_met_h, rollmean_ratio_var_names_met_a, rollmean_ratio_var_names_met_avg_h, rollmean_ratio_var_names_met_avg_a))]

#write rollmean_ratio_df to dbs
DBI::dbWriteTable(connection, "rollmean_ratio_df", rollmean_ratio_df)

#remove rollmean_ratio_df from memory
rm(rollmean_ratio_df)

#get rollmean_df table
rollmean_df_sql <- dplyr::tbl(connection, "rollmean_ratio_df") #get the rollmean_ratio_df table from the db

#join nfl_data_set and rollmean_ratio_df tables
nfl_data_set_sql <- dplyr::tbl(connection, "nfl_data_set") %>%  #get the nfl_data_set table from the db
  dplyr::left_join(rollmean_df_sql, by = c('game_id' = 'game_id')) %>%
  dplyr::collect()

#remove nfl_data_set table from db
DBI::dbRemoveTable(connection, "nfl_data_set")

#insert nfl_data_set table in db with the joined result
DBI::dbWriteTable(connection, "nfl_data_set", nfl_data_set_sql)

#remove nfl_data_set_sql from memory
rm(nfl_data_set_sql)

#delete rollmean_df table
DBI::dbRemoveTable(connection, "rollmean_ratio_df")

DBI::dbDisconnect(connection) #disconnect from database

toc() #stop clock
