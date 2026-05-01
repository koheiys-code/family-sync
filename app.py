"""
[name] app.py
[purpose] create the family-sync web application
[referensce]
    https://uepon.hatenadiary.com/entry/2025/05/18/003609
    https://qiita.com/satsat/items/b4f16d382057e0dd918a
    https://qiita.com/ushi05/items/3e51b218e3e45ef74ff4

written by Kohei Yoshida, 2026/04/23
"""
from datetime import datetime

import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

import finance_manager
from finance_manager import decorate_df, get_sheet_name


CONFIG_YAML_PATH = "config.yaml"
EXPENSES_MANAGER_PARAMS = {
    'database_ss_url': st.secrets["EXPENSES_SS_URLS"]["DATABASE_SS_URL"],
    'income_categories_url': st.secrets["EXPENSES_SS_URLS"]["INCOME_CATEGORIES_URL"],
    'cost_categories_url': st.secrets["EXPENSES_SS_URLS"]["COST_CATEGORIES_URL"],
    'service_account_info': st.secrets["GOOGLE_CREDENTIALS"],
}
START_YEAR = 2026
START_MONTH = 1


@st.cache_resource
def get_expenses_manager(params=EXPENSES_MANAGER_PARAMS):
    return finance_manager.ExpensesManager(**params)


@st.dialog("分類編集")
def apply_edits(expense_manager, edited_df):
    edited_rows = edited_df[edited_df['編集']==True]
    st.dataframe(edited_rows)
    st.write(expense_manager.categories_list)

EM = get_expenses_manager()


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


    st.write(EM.categories_list)


    now = datetime.now()
    now_year, now_month = now.year, now.month
    sheet_name_dict = {}  # '2026年4月': '202604'の形式で保存する
    if now_year == START_YEAR:
        for month in range(START_MONTH, now_month+1):
            repr_name = f'{now_year}年{month}月'
            sheet_name = get_sheet_name(now_year, month)
            sheet_name_dict[repr_name] = sheet_name
    else:
        for year in range(START_YEAR, now.year+1):
            if year == START_YEAR:
                min_month, max_month = START_MONTH, 12
            elif year == now_year:
                min_month, max_month = 1, now_month
            else:
                min_month, max_month = 1, 12
            for month in range(min_month, max_month+1):
                repr_name = f'{year}年{month}月'
                sheet_name = get_sheet_name(year, month)
                sheet_name_dict[repr_name] = sheet_name
    options = sheet_name_dict.keys()
    default_idx = len(sheet_name_dict) - 1
    repr_name = st.selectbox('', options, index=default_idx)
    df = EM.get_database(sheet_name_dict[repr_name])

    if df is not None:
        edit_mode = st.toggle("分類編集", )
        if not edit_mode:
            decorated_df = decorate_df(df, color=True)
            st.dataframe(decorated_df, hide_index=True)
        else:

            editable_df = decorate_df(df, color=False)
            disabled = editable_df.keys()
            editable_df['編集'] = False
            edited_df = st.data_editor(editable_df, disabled=disabled, hide_index=True)
            if st.button('編集'):
                apply_edits(EM, edited_df)
    else:
        st.write('入出金データがありません。')
