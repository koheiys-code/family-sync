"""
[name] items_manager.py
[purpose] 買い物リスト・ストックリストを管理するバックエンドロジック
    - ItemsManager: 買い物リストとストックリストのGoogle Sheets操作を担う
    finance_managerとは異なり、二人が頻繁に同時操作することを考慮して
    st.cache_resourceを使用せず、操作のたびに直接Google Sheetsへアクセスする。
    表示用データはst.session_stateでキャッシュし、APIアクセス回数を抑制する。

written by Kohei Yoshida, 2026/09/15
"""
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread


# Google Sheets APIのアクセス権限スコープ
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

# 買い物リストのシート名
SHOPPING_SHEET_NAME = '買い物リスト'

# ストックリストのシート名
STOCK_SHEET_NAME = 'ストック'

# categoryの列の名前
CATEGORY_COLUMN_NAME = 'カテゴリ'

# 買い物リストの列定義
SHOPPING_COLUMNS = ['品名', 'カテゴリ', 'ストック']

# ストックリストの列定義
STOCK_COLUMNS = ['品名', 'カテゴリ', '登録日']

# 買い物リストの固定カテゴリ
CATEGORIES = ['食品', '日用品', '家具']


class ItemsManager:
    """買い物リストとストックリストを管理するクラス。
    Google Sheetsの同一スプレッドシート内の2つのシートで管理する:
        - 買い物リスト: 購入予定の品目（品名・カテゴリ・ストック対象か否か）
        - ストックリスト: 購入済みのストック品目（品名・カテゴリ・登録日）
    二人が同時に操作する可能性があるため、操作のたびに直接Google Sheetsへ
    アクセスする設計にしている（st.cache_resourceは使用しない）。
    """

    def __init__(self, ss_url, service_account_info,
                 shopping_sheet_name=SHOPPING_SHEET_NAME,
                 stock_sheet_name=STOCK_SHEET_NAME,
                 category_column_name = CATEGORY_COLUMN_NAME,
                 shopping_columns=SHOPPING_COLUMNS,
                 stock_columns=STOCK_COLUMNS,
                 categories=CATEGORIES):
        # Google Sheets APIの認証
        credentials = Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES
        )
        self.client = gspread.authorize(credentials)
        self.ss = self.client.open_by_url(ss_url)
        self.shopping_sheet_name = shopping_sheet_name
        self.stock_sheet_name = stock_sheet_name
        self.category_column_name = category_column_name
        self.shopping_columns = shopping_columns
        self.stock_columns = stock_columns
        self.categories = categories

        # シートが存在しない場合は自動で新規作成する
        self._ensure_worksheets()

    def get_shopping_df(self):
        """買い物リストをDataFrameで取得する。"""
        ws = self.ss.worksheet(self.shopping_sheet_name)
        rows = ws.get_all_values()
        if len(rows) <= 1:
            return pd.DataFrame(columns=self.shopping_columns)
        return pd.DataFrame(rows[1:], columns=self.shopping_columns)

    def get_stock_df(self):
        """ストックリストをDataFrameで取得する。"""
        ws = self.ss.worksheet(self.stock_sheet_name)
        rows = ws.get_all_values()
        if len(rows) <= 1:
            return pd.DataFrame(columns=self.stock_columns)
        return pd.DataFrame(rows[1:], columns=self.stock_columns)

    def each_category_df_generator(self, df):
        """買い物リストまたはストックリストを引数として、categoryごとに分けて返す"""
        for category in self.categories:
            cat_df = df[df[self.category_column_name] == category].copy()
            if not cat_df.empty:
                cat_df = cat_df.drop(self.category_column_name, axis=1)
                yield category, cat_df

    def add_shopping_item(self, name, category, is_stock):
        """買い物リストに1件追加する。

        Args:
            name: 品名
            category: カテゴリ（食品・日用品・家具）
            is_stock: ストック対象か否か（True/False）
        """
        ws = self.ss.worksheet(self.shopping_sheet_name)
        ws.append_row([name, category, str(is_stock)])

    def purchase_items(self, shopping_df, selected_indexes, purchase_date):
        """選択した品目を購入済みにする。
        ストック対象（is_stock=True）の品目はストックリストに追加する。
        ストック対象でない品目はそのまま削除する。

        Args:
            shopping_df: 現在の買い物リストのDataFrame
            selected_indexes: 購入済みにする行のインデックスリスト
            purchase_date: 購入日（datetime.date）
        """
        selected_df = shopping_df.loc[selected_indexes]

        # ストック対象の品目をストックリストに追加する
        stock_ws = self.ss.worksheet(self.stock_sheet_name)
        date_str = purchase_date.strftime('%Y/%m/%d')
        for _, row in selected_df.iterrows():
            if row['ストック'] == 'True':
                stock_ws.append_row([row['品名'], row['カテゴリ'], date_str])

        # 選択した品目を買い物リストから削除する
        new_df = shopping_df.drop(selected_indexes).reset_index(drop=True)
        self._overwrite_shopping(new_df)

    def consume_stock_items(self, stock_df, selected_indexes):
        """選択したストック品目を消費済みにし、買い物リストへ追加する。

        Args:
            stock_df: 現在のストックリストのDataFrame
            selected_indexes: 消費済みにする行のインデックスリスト
        """
        selected_df = stock_df.loc[selected_indexes]

        # 買い物リストへ追加する（ストック=Trueで登録）
        shopping_ws = self.ss.worksheet(self.shopping_sheet_name)
        for _, row in selected_df.iterrows():
            shopping_ws.append_row([row['品名'], row['カテゴリ'], 'True'])

        # 選択した品目をストックリストから削除する
        new_df = stock_df.drop(selected_indexes).reset_index(drop=True)
        self._overwrite_stock(new_df)

    def delete_stock_items(self, stock_df, selected_indexes):
        """選択したストック品目を削除する（買い物リストへは戻さない）。

        Args:
            stock_df: 現在のストックリストのDataFrame
            selected_indexes: 削除する行のインデックスリスト
        """
        new_df = stock_df.drop(selected_indexes).reset_index(drop=True)
        self._overwrite_stock(new_df)

    def _overwrite_shopping(self, df):
        """買い物リストのシートをDataFrameで全て上書きする。"""
        ws = self.ss.worksheet(self.shopping_sheet_name)
        ws.clear()
        values = [self.shopping_columns] + df.values.tolist()
        ws.update(range_name='A1', values=values)

    def _overwrite_stock(self, df):
        """ストックリストのシートをDataFrameで全て上書きする。"""
        ws = self.ss.worksheet(self.stock_sheet_name)
        ws.clear()
        values = [self.stock_columns] + df.values.tolist()
        ws.update(range_name='A1', values=values)

    def _ensure_worksheets(self):
        """買い物リスト・ストックリストのシートが存在しない場合は自動で作成する。"""
        sheet_titles = [ws.title for ws in self.ss.worksheets()]
        if self.shopping_sheet_name not in sheet_titles:
            ws = self.ss.add_worksheet(self.shopping_sheet_name, rows=1000, cols=10)
            ws.update(range_name='A1', values=[self.shopping_columns])
        if self.stock_sheet_name not in sheet_titles:
            ws = self.ss.add_worksheet(self.stock_sheet_name, rows=1000, cols=10)
            ws.update(range_name='A1', values=[self.stock_columns])
