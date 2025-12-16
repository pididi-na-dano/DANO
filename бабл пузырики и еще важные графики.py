import pandas as pd

sdel=pd.read_excel("Сделки_2025-11-25.xlsx")
proj=pd.read_excel("Проектные данные_2025-11-25.xlsx")

import pandas as pd
import numpy as np

# === НАСТРОЙКИ ===
IQR_THRESHOLD = 1.5
MIN_PRICE_UNIT = 1_000_000

# === 0. ПОДГОТОВКА И БЕЗОПАСНОЕ ОБЪЕДИНЕНИЕ (ЧТОБЫ НАЙТИ ДАТУ СТАРТА) ===
sdel_clean = sdel.copy()
proj_clean = proj.copy()

# Названия колонок с датами, которые нам нужны
col_dogovor = 'Дата договора (месяц.год)'
col_reg = 'Дата регистрации (месяц.год)'
col_start = 'Дата начала проекта' # <--- ПРОВЕРЬ, ЧТО В ТАБЛИЦЕ PROJ ОНА НАЗЫВАЕТСЯ ТАК ЖЕ!

# Пытаемся найти дату старта. Если её нет в сделках, тащим из проектов
if col_start not in sdel_clean.columns:
    if col_start in proj_clean.columns:
        # Ищем ключ для объединения (обычно это ID проекта)
        # Пробуем самые частые варианты: 'ID проекта', 'id_project', 'ID_PROJ'
        merge_keys = [k for k in ['ID проекта', 'ID_проекта', 'id_project'] if k in sdel_clean.columns and k in proj_clean.columns]
        
        if merge_keys:
            print(f"✅ Подтягиваем '{col_start}' из таблицы проектов по ключу: {merge_keys[0]}...")
            sdel_clean = sdel_clean.merge(proj_clean[[merge_keys[0], col_start]], on=merge_keys[0], how='left')
        else:
            print(f"⚠️ ВНИМАНИЕ: Не нашел общий ID для объединения таблиц. '{col_start}' может отсутствовать!")
    else:
        print(f"⚠️ ВНИМАНИЕ: Колонки '{col_start}' нет ни в сделках, ни в проектах. Проверь название!")

# === 1. ПРЕДОБРАБОТКА (ETL) ===
def clean_numeric_col(df, col_name):
    if col_name in df.columns:
        val = df[col_name].astype(str)
        val = val.str.replace("\u00a0", "").str.replace(" ", "").str.replace(",", ".")
        return pd.to_numeric(val, errors="coerce").fillna(0)
    return df[col_name] if col_name in df.columns else 0

# Очистка чисел
target_cols = ['Сумма бюджета', 'Суммарная площадь сделок', 'Суммарное количество сделок', 
               'Этаж лота', 'Количество комнат']
for col in target_cols:
    sdel_clean[col] = clean_numeric_col(sdel_clean, col)

# === ОЧИСТКА ДАТ (САМОЕ ВАЖНОЕ) ===
# Собираем все даты, которые есть в датафрейме
all_date_cols = [c for c in [col_dogovor, col_reg, col_start] if c in sdel_clean.columns]

print("\n=== ОТЧЕТ ПО ДАТАМ (ДО ФИЛЬТРАЦИИ) ===")
for col in all_date_cols:
    # 1. Запоминаем сколько было пустых до конвертации
    na_before = sdel_clean[col].isna().sum()
    
    # 2. Конвертируем с dayfirst=True (Российский формат ДД.ММ.ГГГГ)
    # errors='coerce' превратит мусор в NaT, но мы это отследим
    sdel_clean[col] = pd.to_datetime(sdel_clean[col], dayfirst=True, errors='coerce')
    
    # 3. Считаем сколько стало пустых
    na_after = sdel_clean[col].isna().sum()
    lost = na_after - na_before
    
    print(f"Колонка '{col}':")
    if lost > 0:
        print(f"  ❌ БИТЫЙ ФОРМАТ: {lost} значений не удалось прочитать (превратились в NaT).")
    else:
        print(f"  ✅ Все значения успешно распознаны.")

# Расчет удельных метрик
sdel_clean['cnt_safe'] = sdel_clean['Суммарное количество сделок'].replace(0, 1)
sdel_clean['Unit_Area'] = sdel_clean['Суммарная площадь сделок'] / sdel_clean['cnt_safe']
sdel_clean['Unit_Price'] = sdel_clean['Сумма бюджета'] / sdel_clean['cnt_safe']
sdel_clean['Price_m2'] = sdel_clean['Сумма бюджета'] / sdel_clean['Суммарная площадь сделок'].replace(0, np.nan)


# === 2. УМНАЯ ФИЛЬТРАЦИЯ: ПЛОЩАДЬ vs КОМНАТЫ ===
mask_area_rooms_outlier = pd.Series(False, index=sdel_clean.index)
unique_rooms = sorted(sdel_clean['Количество комнат'].unique())

print("\n=== ФИЛЬТРАЦИЯ ПЛОЩАДЕЙ ===")
for room_cnt in unique_rooms:
    idx_room = sdel_clean[sdel_clean['Количество комнат'] == room_cnt].index
    if len(idx_room) == 0: continue
    
    subset_areas = sdel_clean.loc[idx_room, 'Unit_Area']
    Q1 = subset_areas.quantile(0.25)
    Q3 = subset_areas.quantile(0.75)
    IQR = Q3 - Q1
    lower = max(Q1 - (IQR_THRESHOLD * IQR), 10.0)
    upper = Q3 + (IQR_THRESHOLD * IQR)
    
    bad_indices = subset_areas[(subset_areas < lower) | (subset_areas > upper)].index
    mask_area_rooms_outlier.loc[bad_indices] = True

# === 3. ОСТАЛЬНЫЕ ФИЛЬТРЫ ===
Q1_p = sdel_clean['Price_m2'].quantile(0.25)
Q3_p = sdel_clean['Price_m2'].quantile(0.75)
IQR_p = Q3_p - Q1_p
mask_price_outlier = (sdel_clean['Price_m2'] < (Q1_p - 1.5*IQR_p)) | (sdel_clean['Price_m2'] > (Q3_p + 1.5*IQR_p))

mask_cheap = sdel_clean['Unit_Price'] < MIN_PRICE_UNIT

# === 4. СБОРКА ИТОГОВОГО ДАТАСЕТА ===
total_mask = mask_area_rooms_outlier | mask_price_outlier | mask_cheap 
sdel_final = sdel_clean[~total_mask].copy()

# === 5. ФИНАЛЬНЫЙ ОТЧЕТ ===
print("\n" + "="*60)
print(f"ИТОГИ ОЧИСТКИ (Было: {len(sdel_clean)} -> Стало: {len(sdel_final)})")
print("-" * 60)
    
print("-" * 60)
print(f"Удалено строк всего: {total_mask.sum()}")
sdel_final.drop(columns=['cnt_safe'], inplace=True, errors='ignore')


import pandas as pd
import numpy as np
from datetime import timedelta
import warnings

warnings.filterwarnings('ignore')

# ============================================================
# 1) ФУНКЦИЯ ПРЕПРОЦЕССИНГА (ETL)
# ============================================================

def process_real_estate_data(
    proj: str,
    deals: str,
    bank_percentile_range: tuple = (0, 100),
    bank_metric_for_filtering: str = "sq_meters"  # 'count', 'sq_meters', 'money'
):
    """
    ETL для bnMAP / новостройки.

    ВАЖНО:
    1) Sellout считается СТРОГО по ипотечным сделкам.
    2) В предобработке оставляем ТОЛЬКО Комфорт-класс.
       Под "Комфорт" понимается: Комфорт / комфорт / комфорт+ / комфорт класс / класс комфорт и т.п.

    Возвращает:
    df_ml : pd.DataFrame  (колонки совместимы с дальнейшим анализом через rename)
    bank_stats : pd.DataFrame
    report : dict
    """

    print("🚀 ЗАПУСК ОБРАБОТКИ ДАННЫХ (РЕЖИМ: ИПОТЕЧНЫЙ SELLOUT)")
    print(f"   Фильтрация банков-фичей: процентили {bank_percentile_range}, метрика '{bank_metric_for_filtering}'")
    print("   Фильтр классов: ОСТАВЛЯЕМ ТОЛЬКО 'Комфорт' (включая 'Комфорт+', 'Комфорт класс', 'класс комфорт')")

    # =========================================================
    # 0) helpers
    # =========================================================
    def _strip_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        return df

    def clean_numeric_col(df, col_name):
        if col_name in df.columns:
            val = df[col_name].astype(str)
            val = (
                val.str.replace("\u00a0", "", regex=False)
                   .str.replace(" ", "", regex=False)
                   .str.replace(",", ".", regex=False)
            )
            return pd.to_numeric(val, errors="coerce").fillna(0)
        return df[col_name] if col_name in df.columns else 0

    def norm_class_to_key(x) -> str:
        if pd.isna(x):
            return ""
        s = str(x).strip().lower()

        # привести пробелы
        for ch in ["\u00a0", "\t", "\n", "\r"]:
            s = s.replace(ch, " ")
        s = " ".join(s.split())

        # убрать "класс" и шум
        s = s.replace("класс", " ")
        for ch in ["+", "-", "_", "/", "\\", "|", "—", "–", "(", ")", "[", "]", "{", "}", ".", ",", ":", ";"]:
            s = s.replace(ch, " ")
        s = " ".join(s.split())

        # если встречается слово "комфорт" -> комфорт
        if "комфорт" in s:
            return "комфорт"
        return s

    def find_class_col(df: pd.DataFrame):
        candidates = [
            "Класс проекта", "Класс", "Класс жилья", "Класс ЖК", "Класс объекта",
            "Класс_проекта", "КлассПроекта"
        ]
        cols = list(df.columns)
        # 1) точное совпадение
        for c in candidates:
            if c in cols:
                return c
        # 2) “похожее” по подстроке
        low_map = {c: c.lower() for c in cols}
        for c in cols:
            lc = low_map[c]
            if "класс" in lc and ("проект" in lc or "жк" in lc or "жиль" in lc):
                return c
        for c in cols:
            lc = low_map[c]
            if "класс" in lc:
                return c
        return None

    def find_project_col(df: pd.DataFrame):
        # на всякий случай (если в каком-то файле "Название ЖК")
        if "Проект" in df.columns:
            return "Проект"
        if "Название ЖК" in df.columns:
            return "Название ЖК"
        return None

    proj = _strip_columns(proj)
    deals = _strip_columns(deals)

    # =========================================================
    # 2) filter class in proj (comfort only)
    # =========================================================
    class_col_proj = find_class_col(proj)
    if class_col_proj is None:
        print("⚠️ НЕ НАШЁЛ колонку класса в proj. Фильтр по классу НЕ применён.")
    else:
        n_before = len(proj)
        proj["_class_norm"] = proj[class_col_proj].apply(norm_class_to_key)
        print("\n📌 ПРОВЕРКА КЛАССА В PROJ (ТОП-20):")
        try:
            print(proj[class_col_proj].astype(str).value_counts().head(20))
        except Exception:
            pass
        proj = proj[proj["_class_norm"] == "комфорт"].copy()
        proj.drop(columns=["_class_norm"], inplace=True)
        print(f"✅ Фильтр proj по классу: было {n_before}, стало {len(proj)} (Комфорт)")

    # если после фильтра proj пустой — смысла нет продолжать
    if proj.empty:
        print("⛔ После фильтра по классу proj пустой. Проверь названия/значения в колонке класса.")
        return pd.DataFrame(), pd.DataFrame(), {"total": 0, "success": 0, "skipped_young": 0, "skipped_no_sales": 0, "dropped_bad_so": 0}

    # =========================================================
    # 3) numeric cleaning
    # =========================================================
    for col in [
        "Общая проектная площадь",
        "Суммарная площадь сделок",
        "Суммарное количество сделок",
        "Суммарная стоимость сделок",
        "Количество лотов",
    ]:
        if col in proj.columns:
            proj[col] = clean_numeric_col(proj, col)
        if col in deals.columns:
            deals[col] = clean_numeric_col(deals, col)

    if "Суммарное количество сделок" not in deals.columns:
        deals["Суммарное количество сделок"] = 1

    # =========================================================
    # 4) dates
    # =========================================================
    if "Дата договора (месяц.год)" not in deals.columns:
        raise ValueError("В deals нет колонки 'Дата договора (месяц.год)'")

    deals["dt_deal"] = pd.to_datetime(deals["Дата договора (месяц.год)"], dayfirst=True, errors="coerce")
    mask_date_na = deals["dt_deal"].isna()
    if mask_date_na.any():
        deals.loc[mask_date_na, "dt_deal"] = pd.to_datetime(
            "01." + deals.loc[mask_date_na, "Дата договора (месяц.год)"].astype(str),
            dayfirst=True,
            errors="coerce",
        )
    deals = deals.dropna(subset=["dt_deal"]).copy()

    # =========================================================
    # 5) ids / project names
    # =========================================================
    for df in [proj, deals]:
        if "ID корпуса" not in df.columns:
            raise ValueError("Нет колонки 'ID корпуса' в одном из файлов")
        df["ID корпуса"] = df["ID корпуса"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

        proj_col = find_project_col(df)
        if proj_col is not None:
            df[proj_col] = df[proj_col].astype(str).str.strip()
            if proj_col != "Проект":
                df.rename(columns={proj_col: "Проект"}, inplace=True)

    # =========================================================
    # 6) merge deals <- proj mapping (только комфорт!)
    # =========================================================
    class_col_proj = find_class_col(proj)
    cols_map = ["ID корпуса", "Проект"]
    if class_col_proj is not None and class_col_proj in proj.columns:
        cols_map.append(class_col_proj)

    corpus_map = proj[cols_map].drop_duplicates().set_index("ID корпуса")
    n_before_deals = len(deals)
    deals = deals.merge(corpus_map, on="ID корпуса", how="inner")  # INNER = выбрасываем некомфортные корпуса
    print(f"✅ deals после INNER мёржа по корпусам (только Комфорт корпуса): было {n_before_deals}, стало {len(deals)}")

    # === FIX: гарантируем, что в deals есть колонка "Проект" ===
    if "Проект" not in deals.columns:
        if "Проект_y" in deals.columns:
            deals["Проект"] = deals["Проект_y"]
        elif "Проект_x" in deals.columns:
            deals["Проект"] = deals["Проект_x"]
        else:
            raise ValueError("❌ После merge в deals нет колонки 'Проект'")

    # подчистим хвосты
    deals.drop(columns=[c for c in ["Проект_x", "Проект_y"] if c in deals.columns],
            inplace=True, errors="ignore")

    # =========================================================
    # 7) final safety: class filter in deals (если есть)
    # =========================================================
    class_col_deals = find_class_col(deals)
    if class_col_deals is not None:
        n_before = len(deals)
        deals["_class_norm"] = deals[class_col_deals].apply(norm_class_to_key)
        deals = deals[deals["_class_norm"] == "комфорт"].copy()
        deals.drop(columns=["_class_norm"], inplace=True)
        print(f"✅ Фильтр deals по классу: было {n_before}, стало {len(deals)} (Комфорт)")

    # =========================================================
    # 8) mortgage + banks
    # =========================================================
    mortgage_flags = ["да", "yes", "true", "1", "ипотека"]

    if "Название банка" not in deals.columns:
        deals["Название банка"] = "Не указан"
    deals["Название банка"] = deals["Название банка"].fillna("Рассрочка/Кэш").astype(str).str.strip()

    # --- АНАЛИЗ БАНКОВ ---
    grp_cols = {"Суммарное количество сделок": "sum", "Суммарная площадь сделок": "sum"}
    if "Суммарная стоимость сделок" in deals.columns:
        grp_cols["Суммарная стоимость сделок"] = "sum"

    bank_stats = deals.groupby("Название банка").agg(grp_cols).reset_index()
    bank_stats = bank_stats.rename(columns={
        "Суммарное количество сделок": "count",
        "Суммарная площадь сделок": "sq_meters",
        "Суммарная стоимость сделок": "money",
    })
    if "money" not in bank_stats.columns:
        bank_stats["money"] = 0

    total_sq = bank_stats["sq_meters"].sum()
    bank_stats["share_sq_meters"] = (bank_stats["sq_meters"] / total_sq * 100) if total_sq > 0 else 0

    target_metric = bank_metric_for_filtering if bank_metric_for_filtering in bank_stats.columns else "sq_meters"
    threshold_low = np.percentile(bank_stats[target_metric], bank_percentile_range[0])
    threshold_high = np.percentile(bank_stats[target_metric], bank_percentile_range[1])

    bank_stats["is_selected"] = (bank_stats[target_metric] >= threshold_low) & (bank_stats[target_metric] <= threshold_high)
    bank_stats["bank_weight_score"] = np.log1p(bank_stats["sq_meters"])

    selected_banks = bank_stats[bank_stats["is_selected"]]["Название банка"].tolist()
    bank_weights_dict = bank_stats.set_index("Название банка")["bank_weight_score"].to_dict()

    # =========================================================
    # 9) macro
    # =========================================================
    key_rate_data = [
        ("2013-09-13", "2014-03-02", 5.50), ("2014-03-03", "2014-04-24", 7.00), ("2014-04-25", "2014-07-27", 7.50),
        ("2014-07-28", "2014-11-04", 8.00), ("2014-11-05", "2014-12-11", 9.50), ("2014-12-12", "2014-12-15", 10.50),
        ("2014-12-16", "2015-02-01", 17.00), ("2015-02-02", "2015-03-15", 15.00), ("2015-03-16", "2015-05-04", 14.00),
        ("2015-05-05", "2015-06-15", 12.50), ("2015-06-16", "2015-08-02", 11.50), ("2015-08-03", "2016-06-13", 11.00),
        ("2016-06-14", "2016-09-18", 10.50), ("2016-09-19", "2017-03-26", 10.00), ("2017-03-27", "2017-05-01", 9.75),
        ("2017-05-02", "2017-06-18", 9.25), ("2017-06-19", "2017-09-17", 9.00), ("2017-09-18", "2017-10-29", 8.50),
        ("2017-10-30", "2017-12-17", 8.25), ("2017-12-18", "2018-02-11", 7.75), ("2018-02-12", "2018-03-25", 7.50),
        ("2018-03-26", "2018-09-16", 7.25), ("2018-09-17", "2018-12-16", 7.50), ("2018-12-17", "2019-06-16", 7.75),
        ("2019-06-17", "2019-07-28", 7.50), ("2019-07-29", "2019-09-08", 7.25), ("2019-09-09", "2019-10-27", 7.00),
        ("2019-10-28", "2019-12-15", 6.50), ("2019-12-16", "2020-02-09", 6.25), ("2020-02-10", "2020-04-26", 6.00),
        ("2020-04-27", "2020-06-21", 5.50), ("2020-06-22", "2020-07-26", 4.50), ("2020-07-27", "2021-03-21", 4.25),
        ("2021-03-22", "2021-04-25", 4.50), ("2021-04-26", "2021-06-14", 5.00), ("2021-06-15", "2021-07-25", 5.50),
        ("2021-07-26", "2021-09-12", 6.50), ("2021-09-13", "2021-10-24", 6.75), ("2021-10-25", "2021-12-19", 7.50),
        ("2021-12-20", "2022-02-13", 8.50), ("2022-02-14", "2022-02-27", 9.50), ("2022-02-28", "2022-04-10", 20.00),
        ("2022-04-11", "2022-05-03", 17.00), ("2022-05-04", "2022-05-26", 14.00), ("2022-05-27", "2022-06-13", 11.00),
        ("2022-06-14", "2022-07-24", 9.50), ("2022-07-25", "2022-09-18", 8.00), ("2022-09-19", "2022-12-31", 7.50),
        ("2023-01-01", "2023-07-26", 7.50), ("2023-07-27", "2023-08-14", 8.50), ("2023-08-15", "2023-09-17", 12.00),
        ("2023-09-18", "2023-10-29", 13.00), ("2023-10-30", "2023-12-17", 15.00), ("2023-12-18", "2024-07-28", 16.00),
        ("2024-07-29", "2024-09-15", 18.00), ("2024-09-16", "2024-12-27", 19.00), ("2024-12-28", "2025-06-08", 21.00),
    ]

    macro_range = pd.date_range(start="2013-09-13", end="2026-01-01", freq="D")
    macro_df = pd.DataFrame(index=macro_range)
    macro_df["key_rate"] = np.nan
    macro_df["is_subsidy"] = 0

    for start, end, rate in key_rate_data:
        mask = (macro_df.index >= pd.to_datetime(start)) & (macro_df.index <= pd.to_datetime(end))
        macro_df.loc[mask, "key_rate"] = rate

    macro_df["key_rate"] = macro_df["key_rate"].ffill()
    macro_df.loc[(macro_df.index >= "2020-04-17") & (macro_df.index < "2024-07-01"), "is_subsidy"] = 1
    macro_monthly = macro_df["key_rate"].resample("MS").mean()

    def get_macro_features(start_date, end_date):
        subset_daily = macro_df[(macro_df.index >= start_date) & (macro_df.index <= end_date)]
        if subset_daily.empty:
            return np.nan, np.nan, 0, np.nan
        kr_start = subset_daily["key_rate"].iloc[0]
        kr_spread = subset_daily["key_rate"].max() - subset_daily["key_rate"].min()
        sub_share = subset_daily["is_subsidy"].mean()
        subset_monthly = macro_monthly[(macro_monthly.index >= start_date) & (macro_monthly.index <= end_date)]
        kr_mean_monthly = subset_daily["key_rate"].mean() if subset_monthly.empty else subset_monthly.mean()
        return kr_start, kr_spread, sub_share, kr_mean_monthly

    # =========================================================
    # 10) build dataset
    # =========================================================
    corpus_starts = deals.groupby("ID корпуса")["dt_deal"].min().reset_index().rename(columns={"dt_deal": "corpus_start"})
    proj = proj.merge(corpus_starts, on="ID корпуса", how="left")

    temp_proj_starts = proj.groupby("Проект")["corpus_start"].min().reset_index().rename(columns={"corpus_start": "project_start_implied"})
    proj = proj.merge(temp_proj_starts, on="Проект", how="left")
    proj["corpus_start"] = proj["corpus_start"].fillna(proj["project_start_implied"])

    proj_starts = proj.groupby("Проект")["corpus_start"].min().reset_index().rename(columns={"corpus_start": "project_start"})
    MAX_DATE = deals["dt_deal"].max()

    data_list = []
    projects_list = proj_starts["Проект"].unique()

    stats_cnt = {"total": int(len(projects_list)), "success": 0, "skipped_young": 0, "skipped_no_sales": 0, "dropped_bad_so": 0}
    print(f"\n🔄 ОБРАБОТКА ПРОЕКТОВ ({len(projects_list)} шт)...")

    class_col_proj = find_class_col(proj)

    for project in projects_list:
        t0 = proj_starts.loc[proj_starts["Проект"] == project, "project_start"].values[0]
        t0 = pd.to_datetime(t0)
        if pd.isna(t0):
            continue

        t_end_y1 = t0 + timedelta(days=365)
        if (MAX_DATE - t0).days < 365:
            stats_cnt["skipped_young"] += 1
            continue

        valid_corp_y1 = proj[(proj["Проект"] == project) & (proj["corpus_start"] <= t_end_y1)]
        area_planned_y1 = valid_corp_y1["Общая проектная площадь"].sum()

        # ЛОТЫ (если есть)
        if "Количество лотов" in valid_corp_y1.columns:
            lots_planned_y1 = valid_corp_y1["Количество лотов"].sum()
        else:
            lots_planned_y1 = np.nan

        mask_sales_y1 = (deals["Проект"] == project) & (deals["dt_deal"] >= t0) & (deals["dt_deal"] <= t_end_y1)
        deals_subset_y1 = deals[mask_sales_y1]

        # ипотека
        if "Ипотека" not in deals_subset_y1.columns:
            raise ValueError("В deals нет колонки 'Ипотека' — без неё ипотечный sellout не посчитать.")
        mortgage_deals_y1 = deals_subset_y1[deals_subset_y1["Ипотека"].astype(str).str.lower().isin(mortgage_flags)]

        sales_y1 = mortgage_deals_y1["Суммарная площадь сделок"].sum()
        count_y1 = int(len(mortgage_deals_y1))  # количество ипотечных сделок

        if sales_y1 <= 0 or area_planned_y1 <= 0:
            stats_cnt["skipped_no_sales"] += 1
            continue

        kr_start, kr_spread, sub_share, kr_mean = get_macro_features(t0, t_end_y1)
        mort_share = (len(mortgage_deals_y1) / len(deals_subset_y1)) if len(deals_subset_y1) > 0 else 0

        banks_in_project = mortgage_deals_y1[mortgage_deals_y1["Название банка"].isin(selected_banks)]["Название банка"].unique()
        num_banks_filtered = int(len(banks_in_project))
        bank_weighted_index = float(sum(bank_weights_dict.get(b, 0) for b in banks_in_project))

        so_val_pct = float((sales_y1 / area_planned_y1) * 100)

        # класс проекта (из proj, но он уже комфорт)
        if class_col_proj is not None and class_col_proj in proj.columns:
            p_class = proj.loc[proj["Проект"] == project, class_col_proj].iloc[0]
        else:
            p_class = "Комфорт"

        data_list.append({
            "Project": project,
            "Class": p_class,
            "Year_Num": 1,
            "Planned_Area": float(area_planned_y1),
            "Planned_Lots": float(lots_planned_y1) if pd.notna(lots_planned_y1) else np.nan,
            "Sold_Area": float(sales_y1),
            "Deals_Count": int(count_y1),
            "Sellout_Pct": float(so_val_pct),
            "KR_Start": kr_start,
            "KR_Spread": kr_spread,
            "KR_Mean": kr_mean,
            "Subsidy_Share": sub_share,
            "Mortgage_Share": float(mort_share),
            "Num_Banks_Filtered": int(num_banks_filtered),
            "Bank_Index_Weighted": float(bank_weighted_index),
            "Log_Area": float(np.log1p(area_planned_y1)),
        })

    df_ml = pd.DataFrame(data_list)

    if not df_ml.empty:
        n_before = len(df_ml)
        df_ml = df_ml[df_ml["Sellout_Pct"] <= 100].copy()
        stats_cnt["dropped_bad_so"] = int(n_before - len(df_ml))
        stats_cnt["success"] = int(len(df_ml))

    print("\n✅ ГОТОВО!")
    print(f"   Сформировано строк для ML: {len(df_ml)}")
    print(f"   Ошибок Sellout>100%: {stats_cnt['dropped_bad_so']}")

    if "Class" in df_ml.columns and not df_ml.empty:
        print("\n📌 КЛАССЫ В ИТОГЕ df_ml (value_counts):")
        print(df_ml["Class"].astype(str).value_counts().head(20))

    return df_ml, bank_stats, stats_cnt


# ============================================================
# 2) ИМПОРТЫ ДЛЯ АНАЛИЗА И ВИЗУАЛИЗАЦИЙ (КАК У ТЕБЯ)
# ============================================================

import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
from scipy import stats

warnings.filterwarnings('ignore')

# ============================================================
# 0. НАСТРОЙКИ
# ============================================================

sns.set(style="whitegrid", palette="muted")
plt.rcParams["figure.figsize"] = (10, 6)

print("========================================================")
print("СТАРТ АНАЛИЗА bnMAP.pro — НОВЫЙ ПРЕПРОЦЕССИНГ")
print("========================================================\n")

# Файлы (Убедитесь, что они лежат в той же папке)
FILE_PROJ = "Проектные данные_2025-11-25.xlsx"
FILE_DEALS = "Сделки_2025-11-25.xlsx"

# ============================================================
# 2. ПОЛУЧЕНИЕ И ПОДГОТОВКА ДАННЫХ ДЛЯ АНАЛИЗА
# ============================================================

# 2.1 Запускаем правильный препроцессинг
df_ml, bank_stats, report = process_real_estate_data(
    proj,
    sdel_final,
    bank_percentile_range=(0, 100)  # Берем все банки
)

# защитно: если пусто, дальше графики упадут — но ты просил “без ошибок”
# поэтому просто выходим
if df_ml.empty:
    print("\n⛔ df_ml пустой после фильтров. Дальше анализ не строим, чтобы не падать с KeyError.")
    raise SystemExit(0)

# 2.2 Переименовываем колонки, чтобы они совпадали с кодом анализа
final = df_ml.rename(columns={
    "Project": "Проект",
    "Planned_Area": "planned_area",
    "Planned_Lots": "planned_lots",
    "Sold_Area": "sold_area_12m",
    "Deals_Count": "deals_count_12m",
    "Num_Banks_Filtered": "num_banks_12m",
    "Sellout_Pct": "sellout_12m_pct"
}).copy()

final["sellout_12m"] = final["sellout_12m_pct"] / 100.0

# 2.3 Дорасчет логарифмов
final["log_planned_area"] = np.log1p(final["planned_area"])
final["log_deals_12m"] = np.log1p(final["deals_count_12m"])

print("\n=== ИТОГОВЫЙ DATAFRAME ПОСЛЕ ПРАВИЛЬНОГО ETL (ПЕРВЫЕ 5) ===")
print(final.head())

# 2.4 ПОДТЯГИВАЕМ "ОКРУГ" (для Robustness Check в конце)
try:
    raw_deals = pd.read_excel(FILE_DEALS)
    raw_deals.columns = [str(c).strip() for c in raw_deals.columns]

    col_proj_raw = "Проект" if "Проект" in raw_deals.columns else ("Название ЖК" if "Название ЖК" in raw_deals.columns else None)
    col_geo = "Округ"

    if col_proj_raw is not None and col_geo in raw_deals.columns:
        geo_map = raw_deals.groupby(col_proj_raw)[col_geo].agg(
            lambda x: x.mode()[0] if not x.mode().empty else np.nan
        ).reset_index()

        geo_map = geo_map.rename(columns={col_proj_raw: "Проект"})
        final = final.merge(geo_map, on="Проект", how="left")
        print("-> Данные по Округам успешно подтянуты.")
    else:
        print("-> ! Не удалось найти колонку 'Округ' или колонку проекта в исходном файле сделок.")
except Exception as e:
    print(f"-> Ошибка при подтягивании округов: {e}")


# ============================================================
# 8. ВИЗУАЛИЗАЦИЯ (ОСТАВЛЕНО ИЗ ОРИГИНАЛА)
# ============================================================

# Boxplot для сделок
plt.figure()
sns.boxplot(y=final["deals_count_12m"])
plt.title("Boxplot: количество ипотечных сделок за 12 мес (raw)")
plt.ylabel("deals_count_12m")
plt.tight_layout()
plt.show()

# Гистограмма для сделок
plt.figure()
sns.histplot(final["deals_count_12m"].dropna(), bins=20, kde=True)
plt.title("Распределение количества ипотечных сделок (raw)")
plt.xlabel("deals_count_12m")
plt.tight_layout()
plt.show()

# Распределение для log_deals_12m
plt.figure()
sns.histplot(final["log_deals_12m"].dropna(), bins=20, kde=True)
plt.title("Распределение количества ипотечных сделок (log1p)")
plt.xlabel("log(1 + deals_count_12m)")
plt.tight_layout()
plt.show()

# ============================================================
# 9. ГРУППЫ ПРОЕКТОВ ПО КОЛИЧЕСТВУ БАНКОВ
# ============================================================

print("\n=== СТАТИСТИКА ПО БАНКАМ (ПЕРЕД ГРУППИРОВКОЙ) ===")
print(final["num_banks_12m"].describe())

try:
    final["bank_group"] = pd.qcut(
        final["num_banks_12m"],
        q=3,
        labels=["Мало банков", "Средне банков", "Много банков"]
    )
    bank_order = ["Мало банков", "Средне банков", "Много банков"]
    print("\n-> Успешно разделили на 3 группы.")
except ValueError:
    print("\n-> ! Данные слишком однородны для 3 групп. Делим на 2 группы (по Медиане).")
    median_val = final["num_banks_12m"].median()
    final["bank_group"] = np.where(
        final["num_banks_12m"] <= median_val,
        "Мало банков",
        "Много банков"
    )
    bank_order = ["Мало банков", "Много банков"]

print("\n=== ИТОГОВЫЕ ГРУППЫ ===")
print(final["bank_group"].value_counts())

# ============================================================
# 10. ГРАФИКИ: РАСПРЕДЕЛЕНИЕ КОЛИЧЕСТВА БАНКОВ
# ============================================================

plt.figure()
sns.countplot(x="num_banks_12m", data=final)
plt.title("Распределение количества банков в первый год (ипотека)")
plt.xlabel("Количество банков, фактически кредитующих")
plt.ylabel("Количество проектов")
plt.tight_layout()
plt.show()

plt.figure()
sns.countplot(x="bank_group", data=final, order=bank_order)
plt.title("Распределение проектов по группам количества банков")
plt.xlabel("Группа по числу банков")
plt.ylabel("Количество проектов")
plt.tight_layout()
plt.show()

# ============================================================
# 11. BOXPLOT: SELLOUT / СДЕЛКИ vs ГРУППЫ БАНКОВ
# ============================================================

plt.figure()
sns.boxplot(
    x="bank_group",
    y="sellout_12m",
    data=final,
    order=bank_order
)
plt.title("Ипотечный sellout в первый год по группам количества банков")
plt.xlabel("Группа по числу банков")
plt.ylabel("Ипотечный sellout за 12 месяцев (площадь продана / проектная)")
plt.tight_layout()
plt.show()

plt.figure()
sns.boxplot(
    x="bank_group",
    y="deals_count_12m",
    data=final,
    order=bank_order
)
plt.title("Количество ипотечных сделок в первый год по группам количества банков")
plt.xlabel("Группа по числу банков")
plt.ylabel("Количество ипотечных сделок за 12 месяцев")
plt.tight_layout()
plt.show()

# ============================================================
# 12. SCATTER + REGPLOT: RAW vs LOG
# ============================================================

# RAW
plt.figure()
sns.regplot(
    x="num_banks_12m",
    y="deals_count_12m",
    data=final,
    ci=95,
    scatter_kws={"alpha": 0.7}
)
plt.title("Связь: количество банков vs количество ипотечных сделок (raw)")
plt.xlabel("Количество банков, фактически кредитующих проект")
plt.ylabel("Количество ипотечных сделок за 12 мес (raw)")
plt.tight_layout()
plt.show()

# LOG
plt.figure()
sns.regplot(
    x="num_banks_12m",
    y="log_deals_12m",
    data=final,
    ci=95,
    scatter_kws={"alpha": 0.7}
)
plt.title("Связь: количество банков vs количество ипотечных сделок (log1p)")
plt.xlabel("Количество банков, фактически кредитующих проект")
plt.ylabel("log(1 + количество ипотечных сделок за 12 мес)")
plt.tight_layout()
plt.show()

# Также сохраняем график для sellout
plt.figure()
sns.regplot(
    x="num_banks_12m",
    y="sellout_12m",
    data=final,
    ci=95,
    scatter_kws={"alpha": 0.7}
)
plt.title("Связь: количество банков vs ипотечный sellout (12 месяцев)")
plt.xlabel("Количество банков, фактически кредитующих проект")
plt.ylabel("Ипотечный sellout за 12 месяцев")
plt.tight_layout()
plt.show()

# ============================================================
# 13. КОРРЕЛЯЦИИ И HEATMAP (с фокусом на гипотезе)
# ============================================================

corr_vars = [
    "sellout_12m",
    "deals_count_12m",
    "log_deals_12m",
    "num_banks_12m",
    "planned_area",
    "log_planned_area"
]

corr_df = final[corr_vars].corr()

print("\n=== МАТРИЦА КОРРЕЛЯЦИЙ (общая) ===")
print(corr_df)

plt.figure(figsize=(10, 8))
sns.heatmap(
    corr_df,
    annot=True,
    fmt=".2f",
    cmap="Blues",
    vmin=-1,
    vmax=1
)
plt.title("Матрица корреляций (первый год, только ипотека)")
plt.tight_layout()
plt.show()

focus_vars = ["num_banks_12m", "sellout_12m", "log_deals_12m"]
focus_vars = [v for v in focus_vars if v in final.columns]
corr_focus = final[focus_vars].corr()

print("\n=== МАТРИЦА КОРРЕЛЯЦИЙ (фокус на гипотезе) ===")
print(corr_focus)

plt.figure(figsize=(5, 4))
sns.heatmap(
    corr_focus,
    annot=True,
    fmt=".2f",
    cmap="Blues",
    vmin=-1,
    vmax=1
)
plt.title("Матрица корреляций (фокус на гипотезе)")
plt.tight_layout()
plt.show()

# ============================================================
# 14. ПРОВЕРКА НА МУЛЬТИКОЛЛИНЕАРНОСТЬ
# ============================================================

X_vif = final[["num_banks_12m", "log_planned_area", "sellout_12m"]].copy()
X_vif = sm.add_constant(X_vif)

vif_data = pd.DataFrame()
vif_data["Variable"] = X_vif.columns
vif_data["VIF"] = [variance_inflation_factor(X_vif.values, i) for i in range(X_vif.shape[1])]

print("\nVIF для переменных:")
print(vif_data)

# ============================================================
# 15. РЕГРЕССИИ: БАЗОВЫЕ МОДЕЛИ
# ============================================================

reg_df_sellout = final.dropna(subset=["sellout_12m", "num_banks_12m", "log_planned_area"]).copy()
X_sellout = reg_df_sellout[["num_banks_12m", "log_planned_area"]]
X_sellout = sm.add_constant(X_sellout)
y_sellout = reg_df_sellout["sellout_12m"]
model_sellout = sm.OLS(y_sellout, X_sellout).fit()

print("\n========================================================")
print("РЕГРЕССИЯ 1: Ипотечный sellout (12 мес)")
print("Модель: sellout_12m ~ num_banks_12m + log_planned_area")
print("--------------------------------------------------------")
print(model_sellout.summary())

# ============================================================
# 16. ИТОГОВЫЙ ВЕРДИКТ ПО ГИПОТЕЗЕ И POLICY IMPLICATIONS
# ============================================================

beta_sellout = model_sellout.params["num_banks_12m"]
pval_sellout = model_sellout.pvalues["num_banks_12m"]

print("\n========================================================")
print("ИТОГОВЫЙ ВЕРДИКТ ПО ГИПОТЕЗЕ (первый год)")
print("--------------------------------------------------------")

if (pval_sellout < 0.05) and (beta_sellout > 0):
    print("1) Для ипотечного sellout:")
    print(f"   Коэффициент при num_banks_12m = {beta_sellout:.4f} (p={pval_sellout:.4f}) > 0 и значим.")
    print("   → При прочих равных большему количеству банков соответствует более высокий ипотечный sellout.")
else:
    print("1) Для ипотечного sellout статистически значимого положительного эффекта не обнаружено.")
    print(f"   Коэффициент = {beta_sellout:.4f}, p={pval_sellout:.4f}.")

# ============================================================
# BUBBLE-ГРАФИК: sellout vs ипотечная площадь
# ============================================================

bubble_df = final.dropna(subset=["sellout_12m", "sold_area_12m",
                                 "deals_count_12m", "num_banks_12m"]).copy()

plt.figure(figsize=(10, 7))

scatter = sns.scatterplot(
    data=bubble_df,
    x="sellout_12m",
    y="sold_area_12m",
    size="deals_count_12m",
    hue="num_banks_12m",
    sizes=(20, 400),
    palette="Blues",
    alpha=0.8,
    edgecolor="black",
    linewidth=0.5
)

plt.title("Связь между ипотечным sellout и площадью проданных объектов\n"
          "(размер — число ипотечных сделок, цвет — количество банков)")
plt.xlabel("Ипотечный sellout за 12 месяцев (доля площади)")
plt.ylabel("Площадь проданных ипотечных лотов за 12 месяцев, кв. м")

handles, labels = scatter.get_legend_handles_labels()
plt.legend(title="Легенда", loc="upper left", bbox_to_anchor=(1.02, 1))

plt.tight_layout()
plt.show()
