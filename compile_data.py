import os
import glob
import pandas as pd
import re
import json
import csv
from functools import reduce

# compile all data in /data directory into a single csv
os.chdir('data')
# extension = 'csv'
# all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
# combine all files in the list
# combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
# export to csv
# combined_csv.to_csv( "combined_csv.csv", index=False, encoding='utf-8-sig')

# load combined csv into pandas dataframe
matches_df = pd.read_csv("combined_csv.csv")

# initialize empty list to store data
player_stats_list = []
player_stats_columns = ['id', 'wins', 'losses', 'w/l ratio', 'aces', 'double_faults', 'service_points', 'first_serve_in', 'first_serve_won', 'second_serve_won', 'break_points_saved', 'break_points_faced']


# aggregate wins
total_wins_df = matches_df['winner_id'].value_counts().rename_axis('player_id').to_frame('wins')

# aggregate losses
total_losses_df = matches_df['loser_id'].value_counts().rename_axis('player_id').to_frame('losses')

# aggregate aces
win_aces_df = matches_df.groupby('winner_id')[['w_ace']].sum().rename_axis('player_id')
lose_aces_df = matches_df.groupby('loser_id')[['l_ace']].sum().rename_axis('player_id')
combined_aces_df = win_aces_df.join(lose_aces_df)
combined_aces_df['ace'] = combined_aces_df['w_ace'] + combined_aces_df['l_ace']
combined_aces_df.drop(['w_ace', 'l_ace'], axis=1, inplace=True)

# aggregate double faults
win_df_df = matches_df.groupby('winner_id')[['w_df']].sum().rename_axis('player_id')
lose_df_df = matches_df.groupby('loser_id')[['l_df']].sum().rename_axis('player_id')
combined_df_df = win_df_df.join(lose_df_df)
combined_df_df['df'] = combined_df_df['w_df'] + combined_df_df['l_df']
combined_df_df.drop(['w_df', 'l_df'], axis=1, inplace=True)

# aggregate service points
win_svpt_df = matches_df.groupby('winner_id')[['w_svpt']].sum().rename_axis('player_id')
lose_svpt_df = matches_df.groupby('loser_id')[['l_svpt']].sum().rename_axis('player_id')
combined_svpt_df = win_svpt_df.join(lose_svpt_df)
combined_svpt_df['svpt'] = combined_svpt_df['w_svpt'] + combined_svpt_df['l_svpt']
combined_svpt_df.drop(['w_svpt', 'l_svpt'], axis=1, inplace=True)

# aggregate first serves in
win_first_in_df = matches_df.groupby('winner_id')[['w_1stIn']].sum().rename_axis('player_id')
lose_first_in_df = matches_df.groupby('loser_id')[['l_1stIn']].sum().rename_axis('player_id')
combined_first_in_df = win_first_in_df.join(lose_first_in_df)
combined_first_in_df['1stIn'] = combined_first_in_df['w_1stIn'] + combined_first_in_df['l_1stIn']
combined_first_in_df.drop(['w_1stIn', 'l_1stIn'], axis=1, inplace=True)

# aggregate first serve points won
win_first_won_df = matches_df.groupby('winner_id')[['w_1stWon']].sum().rename_axis('player_id')
lose_first_won_df = matches_df.groupby('loser_id')[['l_1stWon']].sum().rename_axis('player_id')
combined_first_won_df = win_first_won_df.join(lose_first_won_df)
combined_first_won_df['1stWon'] = combined_first_won_df['w_1stWon'] + combined_first_won_df['l_1stWon']
combined_first_won_df.drop(['w_1stWon', 'l_1stWon'], axis=1, inplace=True)

# aggregate second serve points won
win_second_won_df = matches_df.groupby('winner_id')[['w_2ndWon']].sum().rename_axis('player_id')
lose_second_won_df = matches_df.groupby('loser_id')[['l_2ndWon']].sum().rename_axis('player_id')
combined_second_won_df = win_second_won_df.join(lose_second_won_df)
combined_second_won_df['2ndWon'] = combined_second_won_df['w_2ndWon'] + combined_second_won_df['l_2ndWon']
combined_second_won_df.drop(['w_2ndWon', 'l_2ndWon'], axis=1, inplace=True)

# aggregate break points won
win_bp_saved_df = matches_df.groupby('winner_id')[['w_bpSaved']].sum().rename_axis('player_id')
lose_bp_saved_df = matches_df.groupby('loser_id')[['l_bpSaved']].sum().rename_axis('player_id')
combined_bp_saved_df = win_bp_saved_df.join(lose_bp_saved_df)
combined_bp_saved_df['bpSaved'] = combined_bp_saved_df['w_bpSaved'] + combined_bp_saved_df['l_bpSaved']
combined_bp_saved_df.drop(['w_bpSaved', 'l_bpSaved'], axis=1, inplace=True)

# aggregate break points faced
win_bp_faced_df = matches_df.groupby('winner_id')[['w_bpFaced']].sum().rename_axis('player_id')
lose_bp_faced_df = matches_df.groupby('loser_id')[['l_bpFaced']].sum().rename_axis('player_id')
combined_bp_faced_df = win_bp_faced_df.join(lose_bp_faced_df)
combined_bp_faced_df['bpFaced'] = combined_bp_faced_df['w_bpFaced'] + combined_bp_faced_df['l_bpFaced']
combined_bp_faced_df.drop(['w_bpFaced', 'l_bpFaced'], axis=1, inplace=True)

# combine into dataframe of player stats

dfs = [total_wins_df, total_losses_df, combined_aces_df, combined_df_df, combined_svpt_df, combined_first_in_df, combined_first_won_df, combined_second_won_df, combined_bp_saved_df, combined_bp_faced_df]

player_stats_df = reduce(lambda left,right: pd.merge(left,right,on='player_id'), dfs)

# append player names
player_names_df = pd.read_csv('atp_players.csv', index_col='player_id')
player_stats_df = player_stats_df.join(player_names_df, on='player_id')

# calculate win/loss ratio
player_stats_df['win/loss'] = player_stats_df['wins'] / player_stats_df['losses']


print(player_stats_df)

player_stats_df.to_csv('output.csv')
