"""
[name] create_yaml.py
[purpose] create a yaml file for user information
[referensce]
    https://qiita.com/bassan/items/ed6d821e5ef680a20872
    https://qiita.com/guunonemodemai/items/1b9ffd8702d4e01075dd
    https://sig9.org/blog/2025/04/24/

written by Kohei Yoshida, 2025/10/13
"""
import csv

import streamlit_authenticator as stauth
import yaml


USER_INFO_PATH = "user_info.csv"
CONFIG_YAML_PATH = "config.yaml"


with open(USER_INFO_PATH, 'r') as f:
    reader = csv.DictReader(f)
    users = list(reader)

hashed_users = {}
for user in users:
    id = user['id']
    hashed_pwd = stauth.Hasher.hash(user['password'])
    hashed_users[id] = {'password' : hashed_pwd}

with open(CONFIG_YAML_PATH, 'r') as f:
    config = yaml.safe_load(f)
config['credentials']['usernames'] = hashed_users
with open(CONFIG_YAML_PATH, 'w') as f:
    yaml.dump(config, f)
