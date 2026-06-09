"""
[name] create_yaml.py
[purpose] ログイン用のパスワードをハッシュ化してconfig.yamlに書き込むスクリプト
    平文パスワードはuser_info.csv（非公開）で管理し、ハッシュ化済みのconfig.yamlのみ
    リポジトリに含めることで、パスワードを安全に公開管理できる。
    パスワードを変更した際はこのスクリプトを再実行してconfig.yamlを更新する。
[referensce]
    https://qiita.com/bassan/items/ed6d821e5ef680a20872
    https://qiita.com/guunonemodemai/items/1b9ffd8702d4e01075dd
    https://sig9.org/blog/2025/04/24/

written by Kohei Yoshida, 2026/0609
"""
import csv

import streamlit_authenticator as stauth
import yaml


# 平文パスワードを記載したCSVファイル（リポジトリには含めない）
# カラム構成: id, password
USER_INFO_PATH = "user_info.csv"

# ハッシュ化済みパスワードを書き込む設定ファイル（リポジトリに含めて良い）
CONFIG_YAML_PATH = "config.yaml"


# user_info.csvからユーザー情報を読み込む
with open(USER_INFO_PATH, 'r') as f:
    reader = csv.DictReader(f)
    users = list(reader)

# 各ユーザーのパスワードをハッシュ化する
hashed_users = {}
for user in users:
    id = user['id']
    hashed_pwd = stauth.Hasher.hash(user['password'])  # bcryptでハッシュ化
    hashed_users[id] = {'password' : hashed_pwd}

# config.yamlのcredentials.usernamesセクションをハッシュ化済みパスワードで上書きする
with open(CONFIG_YAML_PATH, 'r') as f:
    config = yaml.safe_load(f)
config['credentials']['usernames'] = hashed_users
with open(CONFIG_YAML_PATH, 'w') as f:
    yaml.dump(config, f)
