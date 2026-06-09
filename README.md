# family-sync 💰

同棲・家族向けの家計簿Webアプリです。銀行口座のCSVとデビットカードの利用明細CSVをアップロードするだけで、入出金の自動分類・グラフ化・立替管理ができます。

市販の家計簿アプリでは対応していない銀行口座があったことをきっかけに、自分たちの運用に合わせてゼロから開発しました。

---

## 主な機能

- **家計簿タブ**: 月次の入出金一覧表示、大分類・小分類によるカテゴリ編集
- **グラフタブ**: 月次円グラフ・大分類別推移・目的別口座残高・代表口座残高の可視化
- **立替タブ**: 個人の立替記録・給与納入額の計算・精算の家計簿への自動反映
- **データ追加タブ**: 銀行CSV・デビットカードCSVのアップロードと自動取り込み

---

## 技術スタック

| カテゴリ | 使用技術 |
|---|---|
| フロントエンド | [Streamlit](https://streamlit.io/) |
| 認証 | [streamlit-authenticator](https://github.com/mkhorasani/Streamlit-Authenticator) |
| データベース | Google Sheets（[gspread](https://github.com/burnash/gspread)でアクセス） |
| グラフ描画 | matplotlib / seaborn / japanize-matplotlib |
| データ処理 | pandas / numpy |

---

## アーキテクチャ

### クラス構成

```
SpreadSheetOperator         # Google Sheets の基本操作（認証・読み書き）
└── Manager                 # グラフ描画の共通スタイル設定（figure_decorator）
    ├── ExpensesManager     # 家計簿のメインロジック
    └── LendManager         # 個人の立替管理
```

**`ExpensesManager`** が担う主な処理：

- 銀行CSVの読み込みと月次シートへの書き込み（`load_bank_csv`）
- デビットカード明細との突合・内容更新（`update_debit_contents`）
- キーワードマッチングによるカテゴリの自動分類（`_identify_category`）
- 目的別口座への入出金の自動抽出・記録（`_update_purpose_account`）
- 各種グラフの生成（`make_main_pie` / `make_sub_category_trend_plot` / `make_purpose_account_plots` / `make_main_account_plot`）
- 立替精算の家計簿への反映（`process_lend_clearance`）

**`LendManager`** が担う主な処理：

- 立替データの追加・削除（`add_lend` / `full_override`）
- 給与と立替金額からの納入額計算
- 精算確定後の立替データクリア（`clear_lend_data`）

### Google Sheetsの構成

アプリは以下の4種類のスプレッドシートをGoogle Sheets APIで管理します。

| スプレッドシート | 内容 | シート（タブ）構成 |
|---|---|---|
| `database` | 月次の入出金データ | シート名は`202604`形式（年月） |
| `purpose_account` | 目的別口座の入出金履歴 | シート名は口座名（例: `生活防衛費`） |
| `income_categories` | 入金カテゴリ定義 | 大分類・小分類・候補キーワードを列単位で管理 |
| `cost_categories` | 出金カテゴリ定義 | 同上 |

立替データはユーザーごとに別スプレッドシートで管理します。

---

## 設計上の工夫

### デビットカードの日付ズレ問題

銀行口座とデビットカードでは、同じ取引でも記録される日付がズレることがあります。本アプリでは`DEBIT_GAP_DAYS`（デフォルト10日）の範囲内で金額と日付を照合することでこれを吸収しています。

### 目的別口座の動的管理

貯蓄目的ごとに口座を分けて管理しています（例: 生活防衛費・新婚旅行）。口座名はCSVの内容（`普通　円　生活防衛費`）から自動抽出されるため、新しい口座を追加してもコードの変更は不要です。スプレッドシート上に該当するタブが存在しない場合は自動で新規作成されます。

### カテゴリの自動学習

一度カテゴリを手動で設定したキーワードは候補リストに追加され、次回以降は自動で同じカテゴリに分類されます。カテゴリの定義はGoogle Sheetsで永続化されます。

### Google Sheets APIのキャッシュ

`ExpensesManager`は`st.cache_resource`でキャッシュされ、一度取得したワークシートのデータは`called_worksheets`に保持します。これによりAPIへのアクセス回数を最小限に抑えています。データ更新後は`reset_sheet_name=True`でキャッシュを破棄して再取得します。

---

## ファイル構成

```
.
├── app.py                  # Streamlitアプリ本体
├── finance_manager.py      # バックエンドロジック
├── create_yaml.py          # パスワードのハッシュ化・config.yaml生成スクリプト
├── config.yaml             # ハッシュ化済みパスワードを含む認証設定（公開可）
├── requirements.txt        # 依存ライブラリ
├── user_info.csv           # 平文パスワードを記載したCSV（非公開・.gitignore推奨）
└── .streamlit/
    └── secrets.toml        # Google認証情報・スプレッドシートURL（非公開）
```

> `.streamlit/secrets.toml` および `user_info.csv` は機密情報を含むため、リポジトリには含めていません。

---

## パスワード管理

ログイン認証には [streamlit-authenticator](https://github.com/mkhorasani/Streamlit-Authenticator) を使用しています。パスワードはbcryptでハッシュ化したうえで`config.yaml`に保存するため、`config.yaml`はリポジトリに公開しても安全です。

パスワードの設定・変更時は以下の手順で`config.yaml`を更新します。

1. `user_info.csv` にユーザーIDと平文パスワードを記載する
2. `create_yaml.py` を実行してハッシュ化済みパスワードを`config.yaml`に書き込む
3. `user_info.csv` はリポジトリに含めず、ローカルのみで管理する

---

## 動作環境

- Python 3.10以上
- Streamlit 1.56.0

依存ライブラリの詳細は `requirements.txt` を参照してください。
