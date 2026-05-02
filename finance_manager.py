"""
[name] finance_manager.py
[purpose] functions for household expenses
[referensce]
    https://biz.moneyforward.com/work-efficiency/basic/21627/#PythonGoogle

written by Kohei Yoshida, 2026/04/26

TODO:
get_databaseでcalledを更新する
"""
from collections import defaultdict
from datetime import datetime, timedelta
import re
from typing import Iterator

import gspread
from google.oauth2.service_account import Credentials
import numpy as np
import pandas as pd


SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]
BANK_COLUMNS = ['日', '内容', 'is_debit', '出金金額', '入金金額', '残高', '大分類', '小分類']
DEBIT_GAP_DAYS = 10


def get_sheet_name(year: int, month: int) -> str:
    return str(year) + str(month).rjust(2, '0')


def tune_amount(amount) -> str:
    """2,720、120.0などの金額の揺らぎをstrの数字列に整える"""

    if isinstance(amount, str):
        amount = amount.split('.')[0]  # 2,720.00 -> 2,720
        return str(int(re.sub("\\D", "", amount)))  # 2,720 -> 2720
    elif isinstance(amount, (int, float)):
        return str(int(amount))
    return '0'


def get_date_gap(date1: str, date2: str) -> int:
    """20260412、20260425といった年月日の文字列の日数差を取得する"""

    date1 = datetime.strptime(date1, '%Y%m%d')
    date2 = datetime.strptime(date2, '%Y%m%d')
    return abs((date1 - date2) // timedelta(days=1))


def between_days_generator(min_date: str, max_date: str, margin=10) -> Iterator[str]:
    """min_dateとmax_dateの前後にmarginを加え、その間の年月日を一つずつ返す"""

    min_date = datetime.strptime(min_date, '%Y%m%d')
    max_date = datetime.strptime(max_date, '%Y%m%d')
    min_date -= timedelta(days=margin)
    max_date += timedelta(days=margin)
    between_days = (max_date - min_date) // timedelta(days=1)
    date = min_date
    yield datetime.strftime(date, '%Y%m%d')
    for _ in range(between_days):
        date += timedelta(days=1)
        yield datetime.strftime(date, '%Y%m%d')


def decorate_df(df, color=True):  # 見やすいデータフレームを取得する
    decorated_df = df.copy()
    decorated_df['日'] = decorated_df['日'].astype(int)
    decorated_df['金額'] = decorated_df.apply(lambda x: f"-{x['出金金額']}" if x['出金金額']!='0' else f"+{x['入金金額']}", axis=1)
    decorated_df['分類'] = decorated_df.apply(lambda x: x['大分類'] if x['大分類']==x['小分類'] else f"{x['大分類']}/{x['小分類']}", axis=1)
    decorated_df = decorated_df[['日', '内容', '金額', '分類']]
    decorated_df = decorated_df.iloc[::-1, :]
    if color:
        decorated_df = decorated_df.style.map(lambda x: 'color: #0275d8' if x[0]=='+' else 'color: #d9534f', subset=['金額'])
    return decorated_df


class SpreadSheetOperator(object):
    """Goole Spread Sheetを操作するシンプルなクラス"""

    def __init__(self, service_account_info, scopes=SCOPES) -> None:
        credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)
        self.client = gspread.authorize(credentials)

    def get_spread_sheet(self, url):
        return self.client.open_by_url(url)

    def full_update(self, work_sheet, values):  # 元の内容を全て消して、データを差し替える
        work_sheet.clear()
        work_sheet.update(range_name='A1', values=values)

    def get_cell_address(self, row: int, col: int) -> str:  #row=1, col=1 -> A1
        if 1 <= col and col <= 26:
            return chr(64+col) + str(row)
        else:
            raise ValueError(f'エクセルの列が足りません。（col={col}）')

class ExpensesManager(SpreadSheetOperator):
    """家計簿を管理するためのクラス"""

    def __init__(self, database_ss_url, income_categories_url, cost_categories_url,
                 bank_columns=BANK_COLUMNS, debit_gap_days=DEBIT_GAP_DAYS, **kwargs):
        # 親クラスの初期化
        super().__init__(**kwargs)

        # まずは必要なdictionary等を定義しておく
        self.income_categories = {}
        self.cost_categories = {}
        self.categories = {}
        self.repr_category_dict = {}
        self.called_worksheets = {}  # 一度呼び出したワークシートのDataFrameをいれる
        self.bank_columns = bank_columns
        self.debit_gap_days = debit_gap_days

        # スプレッドシートを開く
        self.database_ss = self.get_spread_sheet(database_ss_url)
        self.income_categories_ss = self.get_spread_sheet(income_categories_url)
        self.cost_categories_ss = self.get_spread_sheet(cost_categories_url)

        # カテゴリーデータを取得
        self.income_categories = self._get_categories(self.income_categories_ss)
        self.cost_categories = self._get_categories(self.cost_categories_ss)
        # 統合されたカテゴリデータを作成
        self.categories = dict(**self.income_categories, **self.cost_categories)
        self.repr_category_dict = self._get_repr_category_dict()

    def get_database(self, sheet_name: str):  # エクセルから入出金データを取得する（ex. sheet_name=202604）
        if sheet_name in self.called_worksheets:
            return self.called_worksheets[sheet_name]
        else:
            try:
                database_ws = self.database_ss.worksheet(sheet_name)  # なければここでエラーが起こる
                df = pd.DataFrame(database_ws.get_all_values()[1:], columns=self.bank_columns)  # 取得できればDataFrameで返す
            except gspread.WorksheetNotFound:
                df = None  # シートが見つからなければNoneを返す
            self.called_worksheets[sheet_name] = df
            return df


    def load_bank_csv(self, csv_file):  # 銀行の入出金データでエクセルを更新する
        bank_df = pd.read_csv(csv_file, encoding='shift-jis', dtype=str).fillna(0)
        expenses_dic = defaultdict(list)  # 入出金の情報を該当のシートごとに格納するdict
        for _, row in bank_df.iterrows():  # 銀行からの明細を1行ずつ読み込む
            date = row['日付'].replace('/', '')  # '2026/04/01' -> '20260401'
            sheet_name = date[:6]  # sheet_nameは202604のように管理する
            day = date[6:]
            content = row['内容']
            withdraw = tune_amount(row['出金金額(円)'])  # 金額表示の揺らぎを修正する
            deposit = tune_amount(row['入金金額(円)'])
            balance = tune_amount(row['残高(円)'])

            is_debit = '0'
            if content[:4] == 'デビット':
                is_debit = '1'  # デビットカードの明細はマーキング
            elif content[:6] == 'ポイント利用':
                content = 'ポイント利用'  # 'ポイント利用 (数字)'の表示を'ポイント利用'に統一
            main_category, sub_category = self._identify_category(content)

            expenses_dic[sheet_name].append([
                day,  # 日
                content,  # 内容
                is_debit,  # is_debit
                withdraw,  # 出金金額
                deposit,  # 入金金額
                balance,  # 残高
                main_category,  # 大分類
                sub_category,  # 小分類
            ])
        for sheet_name, values in expenses_dic.items():  # シートごとにエクセルをアップデートする
            values = values[::-1]  # 日付を昇順にする
            pre_df = self.get_database(sheet_name)
            if pre_df is not None:
                post_df = pd.DataFrame(values, columns=self.bank_columns)
                new_df = pd.concat([pre_df, post_df], ignore_index=True)  # 元データと新データを統合
                new_df = new_df.drop_duplicates(subset=['日', '出金金額', '入金金額','残高'])  # 同じ取引を削除
                values = [self.bank_columns] + new_df.values.tolist()  # エクセル用に成形
            else:
                values = [self.bank_columns] + values
                self.database_ss.add_worksheet(sheet_name, rows=5000, cols=26)  # シートを新規作成
            self.full_update(self.database_ss.worksheet(sheet_name), values)

    def update_debit_contents(self, debit_csv_path):  # デビットカードの明細で入出金データを更新する
        # まずはデビットカードの明細を取得し、同じ金額ごとに明細の情報（金額、内容）をまとめる
        debit_df = pd.read_csv(debit_csv_path, encoding='shift-jis', dtype=str).fillna(0)
        min_date, max_date = 99999999, 0  # 同じ取引でも銀行とカードの日付はズレる -> 後でサーチする際の範囲を決める
        same_withdraw_debit_dict = defaultdict(list)  # 金額はブレないことから同じ金額の明細を集める
        for _, row in debit_df.iterrows():
            date = row['お取引日'].replace('/', '')  # '20260401'
            int_date = int(date)
            min_date = min(min_date, int_date)
            max_date = max(max_date, int_date)
            content = row['お取引内容']
            withdraw = tune_amount(row['お取引金額'])  # 金額表示の揺らぎを修正する
            same_withdraw_debit_dict[withdraw].append({'date': date, 'content': content})

        # 検索範囲の日付を1日ずつ取得し、デビットカードの履歴と照合する
        min_date, max_date = str(min_date), str(max_date)
        update_batches = defaultdict(list)  # シートごとにbatchを作成して更新する
        # 検索範囲の日付を1日ずつ取得
        for date in between_days_generator(min_date, max_date, margin=self.debit_gap_days):
            sheet_name = date[:6]
            day = date[6:]
            df = self.get_database(sheet_name)
            if df is None:  # ワークシートがなければ次のループへ
                continue

            # DataFrameの中で検索日かつデビット履歴ものを呼び出す
            content_col = self.bank_columns.index('内容') + 1  # 内容が何列目に格納されているかを取得（エクセルは1から数える）
            is_debit_col = self.bank_columns.index('is_debit') + 1  # is_debitが...
            main_category_col = self.bank_columns.index('大分類') + 1  # 大分類が...
            sub_category_col = self.bank_columns.index('小分類') + 1  # 小分類が...
            for df_idx, row in df[(df['日']==day) & (df['is_debit']=='1')].iterrows():
                excel_idx = df_idx + 2  # dataframeからエクセルで行のカウントが2つズレる
                withdraw = row['出金金額']
                for idx, candidate in enumerate(same_withdraw_debit_dict[withdraw]):
                    candidate_date = candidate['date']  # 同じ金額の履歴が内容修正の候補になる
                    date_gap = get_date_gap(date, candidate_date)  # 日付のgapを計算する
                    if date_gap <= self.debit_gap_days:  # 設定した日数より少なければ更新する
                        content_address = self.get_cell_address(excel_idx, content_col)  # エクセルでの住所を取得（ex. A1）
                        is_debit_address = self.get_cell_address(excel_idx, is_debit_col)
                        main_category_address = self.get_cell_address(excel_idx, main_category_col)
                        sub_category_address = self.get_cell_address(excel_idx, sub_category_col)
                        content = candidate['content']
                        main_category, sub_category = self._identify_category(content)
                        update_batches[sheet_name].append({'range': content_address, 'values': [[content]]})
                        update_batches[sheet_name].append({'range': is_debit_address, 'values': [['0']]})
                        update_batches[sheet_name].append({'range': main_category_address, 'values': [[main_category]]})
                        update_batches[sheet_name].append({'range': sub_category_address, 'values': [[sub_category]]})
                        df.loc[df_idx, '内容'] = content  # dfの内容も変えておく
                        df.loc[df_idx, 'is_debit'] = '0'
                        df.loc[df_idx, '大分類'] = main_category
                        df.loc[df_idx, '小分類'] = sub_category
                        same_withdraw_debit_dict[withdraw].pop(idx)  # マッチしたものは候補から消去する

        # シートごとにbatchで更新することで、効率よく、エラーを減らせる
        for sheet_name, update_batch in update_batches.items():
            self.database_ss.worksheet(sheet_name).batch_update(update_batch)


    def update_categories(self, sheet_name='sheet1'):  # カテゴリーをまとめたエクセルを更新する
        items = [(self.cost_categories, self.cost_categories_ss),
                 (self.income_categories, self.income_categories_ss)]
        for categories, spread_sheet in items:
            first_column = np.array([['大分類', '小分類', '候補']]).T.tolist()
            batch = [{'range': 'A1:A3', 'values': first_column}]
            col = 2
            for main, sub_categories in categories.items():
                for sub, candidates in sub_categories.items():
                    values = [main, sub] + candidates
                    values = np.array([values]).T.tolist()
                    start_address = self.get_cell_address(1, col)
                    end_address = self.get_cell_address(len(values), col)
                    range_ = f'{start_address}:{end_address}'
                    batch.append({'range': range_, 'values': values})
                    col += 1
            ws = getattr(spread_sheet, sheet_name)
            ws.clear()
            ws.batch_update(batch)


    def _get_categories(self, spread_sheet, sheet_name='sheet1') -> dict:  # エクセルからdictに成形して返す
        categories = defaultdict(dict)
        # work_sheet = getattr(spread_sheet, sheet_name)
        work_sheet = spread_sheet.worksheet(sheet_name)
        all_values = work_sheet.get_all_values()  # エクセルの全てのセルを取得
        for row in np.array(all_values).T[1:]:
            main = row[0]
            sub = row[1]
            candidates = []
            for candidate in row[2:]:
                if candidate:
                    candidates.append(candidate)
                else:
                    break
            categories[main][sub] = candidates
        return categories


    def _get_repr_category_dict(self):
        repr_category_dict = {}
        for main, sub_categories in self.categories.items():
            for sub in sub_categories.keys():
                if main == sub:
                    key = main
                else:
                    key = f'{main}/{sub}'
                repr_category_dict[key] = {'main': main, 'sub': sub}
        return repr_category_dict


    def _identify_category(self, content: str, uncategorized='未分類') -> tuple[str, str]:
        """categoriesを使ってcontentの大分類と小分類を取得する"""
        for main, sub_categories in self.categories.items():
            for sub, candidates in sub_categories.items():
                if content in candidates:
                    return main, sub
        return uncategorized, uncategorized


if __name__ == '__main__':
    pass
