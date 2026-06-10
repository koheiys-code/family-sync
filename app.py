"""
[name] app.py
[purpose] 家計簿アプリ family-sync のフロントエンド（Streamlit）
[referensce]
    https://uepon.hatenadiary.com/entry/2025/05/18/003609
    https://qiita.com/satsat/items/b4f16d382057e0dd918a
    https://qiita.com/ushi05/items/3e51b218e3e45ef74ff4

written by Kohei Yoshida, 2026/06/09
構成:
    - 家計簿タブ: 月次の入出金一覧と分類編集
    - グラフタブ: 月次円グラフ・大分類推移・目的別口座残高・代表口座残高
    - 立替タブ: 個人の立替管理と精算
    - データ追加タブ: 銀行CSV・デビットCSVのアップロード
"""
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

import finance_manager


# 認証設定ファイルのパス
CONFIG_YAML_PATH = "config.yaml"

# ExpensesManagerの初期化パラメータ（Streamlit secretsから読み込む）
EXPENSES_MANAGER_PARAMS = {
    'database_ss_url': st.secrets["EXPENSES_SS_URLS"]["DATABASE_SS_URL"],
    'purpose_account_ss_url': st.secrets["EXPENSES_SS_URLS"]["PURPOSE_ACCOUNT_SS_URL"],
    'income_categories_url': st.secrets["EXPENSES_SS_URLS"]["INCOME_CATEGORIES_URL"],
    'cost_categories_url': st.secrets["EXPENSES_SS_URLS"]["COST_CATEGORIES_URL"],
    'service_account_info': st.secrets["GOOGLE_CREDENTIALS"],
    'start_year': 2026,
    'start_month': 4,
}
# 立替スプレッドシートのURL辞書（キーはユーザー名）
LEND_URL_DICT = st.secrets["LEND_URLS"]

# LendManagerの初期化パラメータ
LEND_MANAGER_PARAMS = {
    'service_account_info': st.secrets["GOOGLE_CREDENTIALS"],
}

# 目的別口座の目標金額辞書（キーは口座名、値は目標金額）
# secrets.tomlの[PURPOSE_ACCOUNT_GOALS]セクションで管理する
PURPOSE_ACCOUNT_GOALS = st.secrets["PURPOSE_ACCOUNT_GOALS"]

# 給与から家計への納入割合（例: 0.7 = 手取りの70%を納入する）
PAYMENT_RATIO = 0.7


@st.cache_resource
def get_expenses_manager(params=EXPENSES_MANAGER_PARAMS):
    """ExpensesManagerをキャッシュして返す。
    st.cache_resourceによりアプリ起動時に一度だけ初期化され、
    Google Sheets APIへの過剰なアクセスを防ぐ。
    """
    return finance_manager.ExpensesManager(**params)


def get_lend_managers_dict(user_name, lend_url_dict=LEND_URL_DICT, params=LEND_MANAGER_PARAMS):
    """全ユーザーのLendManagerを生成し、{ユーザー名: LendManager}形式の辞書を返す。
    ログイン中のユーザーのみpermission=Trueとなり、自分の立替の編集・精算が可能になる。
    """
    lend_managers_dict = {}
    lower_user_name = user_name.lower()
    for name, url in lend_url_dict.items():
        name = name.lower()
        permission = (lower_user_name == name)  # ログインユーザーのみ編集権限を付与
        LM = finance_manager.LendManager(name, url, permission=permission, **params)
        lend_managers_dict[name] = LM
    return lend_managers_dict


def initialize_session_state():
    """session_stateの初期値を設定する。
    副業収入の入力枠数など、ダイアログをまたいで保持したい状態を管理する。
    """
    if "sub_job_count" not in st.session_state:
        st.session_state.sub_job_count = 0


@st.dialog('編集モード')
def apply_edits(expense_manager, sheet_name, edited_df, edit_type):
    """チェックした行のカテゴリを一括変更するダイアログ。"""
    repr_category_dict = expense_manager.get_repr_category_dict(edit_type)
    options = repr_category_dict.keys()
    repr_category = st.selectbox('', options)
    edited_rows = edited_df[edited_df['編集']==True]
    st.dataframe(edited_rows, hide_index=True)
    category_info = repr_category_dict[repr_category]
    main, sub = category_info['main'], category_info['sub']
    st.write(f'分類を{repr_category}に変更しますか？')
    if st.button('確定'):
        expense_manager.update_categories(sheet_name, edited_rows.index, main ,sub, edit_type)
        st.session_state.show_dialog = False
        st.session_state.edit_mode = False
        st.rerun()


@st.dialog('立替を追加')
def add_lend(lend_manager, expense_manager):
    """立替データを1件追加するダイアログ。"""
    name = lend_manager.name
    lend_date = st.date_input('日にち')
    repr_date = lend_date.strftime('%Y/%m/%d')
    content = st.text_input('内容')
    payment = st.number_input('金額', min_value=0, value=0, step=1)
    repr_category_dict = expense_manager.get_repr_category_dict('出金')
    options = repr_category_dict.keys()
    repr_category = st.selectbox('分類', options)
    st.write(f'{name}は{repr_date}に{content}（{repr_category}）のために{payment:,}円を払いましたか？')
    if content and payment and st.button('確定'):
        category_info = repr_category_dict[repr_category]
        main, sub = category_info['main'], category_info['sub']
        lend_manager.add_lend(lend_date, content, payment, main, sub)
        st.session_state.show_dialog = False
        st.rerun()


@st.dialog('消去')
def apply_delete(lend_manager, deletable_df):
    """チェックした立替データを削除するダイアログ。"""
    delete_rows = deletable_df[deletable_df['消去']==True]
    st.dataframe(delete_rows, hide_index=True)
    if st.button('上記の項目を消去しますか？'):
        new_df = lend_manager.lend_df[deletable_df['消去']==False]
        lend_manager.full_override(new_df)
        st.session_state.show_dialog = False
        st.session_state.delete_mode = False
        st.rerun()


@st.dialog('納入額計算')
def calc_monthly_payment(lend_manager, expense_manager, ratio=PAYMENT_RATIO):
    """給与と立替金額から今月の納入額を計算するダイアログ。
    納入額 = 合計手取り * ratio - 立替合計金額
    精算確定ボタンを押すと家計簿への反映と立替データのクリアが実行される。
    """
    main_salary = st.number_input('本業の手取り', min_value=0, value=0, step=1)
    if st.button('+副業の入力枠を追加'):
        st.session_state.sub_job_count += 1
    sub_job_incomes = []
    for i in range(st.session_state.sub_job_count):
        income = st.number_input(f'副業({i+1})の収益', min_value=0, value=0, step=1)
        sub_job_incomes.append(income)
    salary_sum = sum([main_salary] + sub_job_incomes)
    salary_sum_postscript = ''
    if sub_job_incomes:
        salary_sum_postscript = f' ( = {main_salary:,}'
        for sub_job_income in sub_job_incomes:
            salary_sum_postscript += f' + {sub_job_income:,}'
        salary_sum_postscript += ' )'

    cost_sum = lend_manager.cost_sum
    st.write(f'合計手取り: {int(salary_sum):,}円' + salary_sum_postscript)
    st.write(f'立替金額: {int(cost_sum):,}円')
    if salary_sum:
        monthly_payment = salary_sum * ratio - cost_sum
        st.write(f'納入額: {int(monthly_payment):,}円 ( = {salary_sum:,} * {ratio} - {cost_sum:,})')
        st.warning("※銀行への入金を行った後、以下のボタンを押すと家計簿に立替が反映され、立替リストがクリアされます。")
        # lend_manager.lend_dfを使用して、expense_managerに反映させる。

        if cost_sum > 0:
            if st.button('精算を確定して家計簿に反映'):
                expense_manager.process_lend_clearance(lend_manager.lend_df, lend_manager.name)
                lend_manager.clear_lend_data()
                st.success("家計簿への反映と立替データのクリアが完了しました！")
                st.session_state.sub_job_count = 0  # 副業枠のカウントもリセット
                st.rerun()
        else:
            st.info("立替金額が0円のため、家計簿への精算処理は不要です。")

# ユーザー設定の読み込み
with open(CONFIG_YAML_PATH) as f:
    config = yaml.load(f, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    credentials=config['credentials'],
    cookie_expiry_days=config['cookie']['expiry_days'],
)

# アプリ起動のタイミングでEMを作成してキャッシュ化しておく
# ログイン前に実行することで、ログイン後の初回表示を高速化する
EM = get_expenses_manager()

# ログイン画面の表示
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
    if "lend_managers_dict" not in st.session_state:
            st.session_state.lend_managers_dict = get_lend_managers_dict(user_name)
    lend_managers_dict = st.session_state.lend_managers_dict
    initialize_session_state()

    st.title(':tada: family-sync')

    expenses_tab, fig_tab, lend_tab, upload_tab = st.tabs(['家計簿', 'グラフ', '立替', 'データ追加'])

    options = EM.sheet_name_dict.keys()
    default_idx = len(EM.sheet_name_dict) - 1
    with expenses_tab:
        repr_name = st.selectbox('', options, index=default_idx, key='expenses_options')
        sheet_name = EM.sheet_name_dict[repr_name]
        df = EM.get_database(sheet_name)

        if df is not None:
            edit_mode = st.toggle('分類編集', key='edit_mode')
            if not edit_mode:
                decorated_df = EM.decorate_df(sheet_name, color=True)
                st.dataframe(decorated_df, hide_index=True)
            else:
                edit_type = st.radio('', ['出金', '入金'])
                editable_df = EM.decorate_df(sheet_name, edit_type=edit_type, color=False)
                if editable_df is None:
                    st.write(f'{edit_type}データがありません。')
                else:
                    disabled = editable_df.keys()
                    editable_df['編集'] = False
                    if st.checkbox('未分類のみ'):
                        editable_df = editable_df[editable_df['分類']=='未分類']
                    edited_df = st.data_editor(editable_df, disabled=disabled, hide_index=True)
                    if st.button('編集'):
                        apply_edits(EM, sheet_name, edited_df, edit_type)
        else:
            st.info('入出金データがありません。')

    with fig_tab:
        st.subheader('🍕 月次内訳（円グラフ）')
        repr_name = st.selectbox('', options, index=default_idx, key='fig_options')
        sheet_name = EM.sheet_name_dict[repr_name]
        cost_main_pie = EM.make_main_pie(sheet_name, '出金')
        income_main_pie = EM.make_main_pie(sheet_name, '入金')
        if cost_main_pie is None and income_main_pie is None:
            st.info('集計可能な履歴がありません。')
        else:
            left, right = st.columns(2)
            with left:
                if cost_main_pie is not None:
                    st.pyplot(cost_main_pie)
            with right:
                if income_main_pie is not None:
                    st.pyplot(income_main_pie)

        st.write('---')
        st.subheader('🔍 大分類別の推移')
        main_categories = list(EM.categories.keys())
        selected_main_cat = st.selectbox('', main_categories)
        plot_type = st.radio('', ['金額', '割合'], horizontal=True)
        is_ratio_display = (plot_type == '割合')
        trend_plot = EM.make_sub_category_trend_plot(selected_main_cat, is_ratio_display)
        if trend_plot is None:
            st.info(f'集計可能な履歴がありません。')
        else:
            st.pyplot(trend_plot)

            # 選択した年月・大分類の小分類ごとの明細を表示する
            repr_name = st.selectbox('明細を表示する月を選択', options, index=default_idx)
            sheet_name = EM.sheet_name_dict[repr_name]
            detail_df = EM.get_database(sheet_name)
            if detail_df is not None and not detail_df.empty:
                detail_df = detail_df[detail_df['大分類'] == selected_main_cat].copy()
                # 金額列を作成（出金金額・入金金額のどちらか0でない方を使用）
                detail_df['金額'] = detail_df.apply(
                    lambda x: int(x['出金金額']) if x['出金金額'] != '0' else int(x['入金金額']), axis=1)
                # 小分類ごとにサブヘッダー＋表を表示する
                for sub_cat, sub_df in detail_df.groupby('小分類'):
                    st.markdown(f'###### {sub_cat}')
                    display_df = sub_df[['日', '内容', '金額']].reset_index(drop=True)
                    st.dataframe(display_df, hide_index=True, height=200)

        st.write('---')
        st.subheader('🏦 目的別口座の残高推移')
        purpose_account_plots = EM.make_purpose_account_plots()
        if not purpose_account_plots:
            st.info('目的別口座のデータがありません。')
        else:
            for account_name, fig in purpose_account_plots.items():
                goal = PURPOSE_ACCOUNT_GOALS.get(account_name)
                goal_text = f'（目標 {goal:,}円）' if goal else ''
                # goalがある場合とない場合でのコーディング
                st.markdown(f'##### {account_name}{goal_text}')
                st.pyplot(fig)

        st.write('---')
        st.subheader('📈 代表口座の推移')
        main_account_plot = EM.make_main_account_plot()
        if main_account_plot is not None:
            st.pyplot(main_account_plot)
        else:
            st.info(f'集計可能な履歴がありません。')

    with lend_tab:
        user_key = ''
        for name, LM in lend_managers_dict.items():
            cost_sum = LM.cost_sum
            decorate_df = LM.get_decorated_df()
            st.write(f'{name}の立替合計金額は{cost_sum:,}円です。')
            if not LM.permission:
                st.dataframe(decorate_df, hide_index=True)
            else:
                user_key = name
                if st.button(f'{user_key}の納入額を計算'):
                    user_LM = lend_managers_dict[user_key]
                    calc_monthly_payment(user_LM, EM)

                if decorate_df.empty:
                    if st.button('追加', key=f'{name}_add_lend'):
                        add_lend(LM, EM)
                    st.dataframe(decorate_df, hide_index=True)
                else:
                    delete_mode = st.toggle('消去', key='delete_mode')
                    if delete_mode:
                        deletable_df = decorate_df.copy()
                        disabled = deletable_df.keys()
                        deletable_df['消去'] = False
                        deletable_df = st.data_editor(deletable_df, disabled=disabled, hide_index=True)
                        if st.button('消去'):
                            apply_delete(LM, deletable_df)
                    else:
                        if st.button('追加', key=f'{name}_add_lend'):
                            add_lend(LM, EM)
                        st.dataframe(decorate_df, hide_index=True)


    with upload_tab:
        with st.form('利用履歴更新フォーム', clear_on_submit=True):
            files = st.file_uploader('利用履歴をアップロード', type="csv", accept_multiple_files=True)
            if st.form_submit_button('実行'):
                for file in files:
                    # ファイル名の先頭でCSVの種別を判定する
                    # nyushukinmeisai_*.csv -> 銀行の入出金明細
                    # meisai_*.csv -> デビットカードの利用明細
                    identifier = file.name.split('_')[0]
                    if identifier == 'nyushukinmeisai':
                        EM.load_bank_csv(file)
                    elif identifier == 'meisai':
                        EM.update_debit_contents(file)
                    else:
                        st.write(f'読み込めませんでした。 {file.name}')
                st.rerun()
