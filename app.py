"""
[name] app.py
[purpose] create the family-sync web application
[referensce]
    https://uepon.hatenadiary.com/entry/2025/05/18/003609
    https://qiita.com/satsat/items/b4f16d382057e0dd918a
    https://qiita.com/ushi05/items/3e51b218e3e45ef74ff4

written by Kohei Yoshida, 2026/04/23
"""
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

from finance_manager import ExpensesManager

CONFIG_YAML_PATH = "config.yaml"
EXPENSES_MANAGER_PARAMS = {
    'database_ss_url': st.secrets["EXPENSES_SS_URLS"]["DATABASE_SS_URL"],
    'categories_ss_url': st.secrets["EXPENSES_SS_URLS"]["CATEGORIES_SS_URL"],
    'service_account_info': st.secrets["GOOGLE_CREDENTIALS"],
}
EM = ExpensesManager(**EXPENSES_MANAGER_PARAMS)

# ユーザー設定の読み込み
with open(CONFIG_YAML_PATH) as f:
    config = yaml.load(f, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    credentials=config['credentials'],
    cookie_expiry_days=config['cookie']['expiry_days'],
)

authenticator.login()
if st.session_state["authentication_status"] is None:
    # デフォルト
    st.warning('Please enter your username and password')

elif st.session_state["authentication_status"] is False:
    # ログイン失敗
    st.error('Username/password is incorrect')

elif st.session_state['authentication_status']:
    # ログイン成功
    user_name = st.session_state['username']

    st.title(':tada: family-sync')
