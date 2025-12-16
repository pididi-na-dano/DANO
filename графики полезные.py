import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
from scipy import stats
from datetime import timedelta
import warnings

warnings.filterwarnings('ignore')

# Настройки графиков
sns.set(style="whitegrid", palette="muted")
plt.rcParams["figure.figsize"] = (10, 6)

# ============================================================
# 1. ЗАГРУЗКА И БЕЗОПАСНОЕ ОБЪЕДИНЕНИЕ
# ============================================================
print("⏳ Загрузка файлов...")
# Убедитесь, что файлы лежат рядом со скриптом
try:
    sdel = pd.read_excel("Сделки_2025-11-25.xlsx")
    proj = pd.read_excel("Проектные данные_2025-11-25.xlsx")
except FileNotFoundError:
    print("❌ Ошибка: Файлы Excel не найдены. Проверьте названия файлов.")
    raise SystemExit

# === НАСТРОЙКИ ОЧИСТКИ ===
IQR_THRESHOLD = 1.5
MIN_PRICE_UNIT = 1_000_000

sdel_clean = sdel.copy()
proj_clean = proj.copy()

# Удаляем лишние пробелы в названиях колонок
sdel_clean.columns = [str(c).strip() for c in sdel_clean.columns]
proj_clean.columns = [str(c).strip() for c in proj_clean.columns]

col_dogovor = 'Дата договора (месяц.год)'
col_reg = 'Дата регистрации (месяц.год)'
col_start = 'Дата начала проекта'

# Подтягиваем дату старта, если её нет
if col_start not in sdel_clean.columns:
    if col_start in proj_clean.columns:
        merge_keys = [k for k in ['ID проекта', 'ID_проекта', 'id_project', 'ID корпуса'] 
                      if k in sdel_clean.columns and k in proj_clean.columns]
        if merge_keys:
            # Используем первый найденный ключ
            key = merge_keys[0]
            print(f"✅ Подтягиваем '{col_start}' из таблицы проектов по ключу: {key}...")
            # Берем уникальные даты для ключа, чтобы не дублировать строки
            proj_dates = proj_clean[[key, col_start]].drop_duplicates(subset=[key])
            sdel_clean = sdel_clean.merge(proj_dates, on=key, how='left')
    else:
        print(f"⚠️ ВНИМАНИЕ: Колонки '{col_start}' нет. Анализ по годам может быть неточным.")

# ============================================================
# 2. ПРЕДОБРАБОТКА ЧИСЕЛ И ДАТ (ETL)
# ============================================================
def clean_numeric_col(df, col_name):
    if col_name in df.columns:
        val = df[col_name].astype(str)
        val = val.str.replace("\u00a0", "").str.replace(" ", "").str.replace(",", ".")
        return pd.to_numeric(val, errors="coerce").fillna(0)
    return df[col_name] if col_name in df.columns else 0

target_cols = ['Сумма бюджета', 'Суммарная площадь сделок', 'Суммарное количество сделок', 
               'Этаж лота', 'Количество комнат', 'Общая проектная площадь', 'Количество лотов']

for col in target_cols:
    if col in sdel_clean.columns:
        sdel_clean[col] = clean_numeric_col(sdel_clean, col)
    if col in proj_clean.columns:
        proj_clean[col] = clean_numeric_col(proj_clean, col)

# Гарантируем, что количество сделок везде проставлено
if 'Суммарное количество сделок' not in sdel_clean.columns:
    sdel_clean['Суммарное количество сделок'] = 1
else:
    sdel_clean['Суммарное количество сделок'] = sdel_clean['Суммарное количество сделок'].replace(0, 1)

# Очистка дат
all_date_cols = [c for c in [col_dogovor, col_reg, col_start] if c in sdel_clean.columns]
for col in all_date_cols:
    sdel_clean[col] = pd.to_datetime(sdel_clean[col], dayfirst=True, errors='coerce')

# Расчет метрик для фильтрации выбросов
sdel_clean['Unit_Area'] = sdel_clean['Суммарная площадь сделок'] / sdel_clean['Суммарное количество сделок']
sdel_clean['Unit_Price'] = sdel_clean['Сумма бюджета'] / sdel_clean['Суммарное количество сделок']
sdel_clean['Price_m2'] = sdel_clean['Сумма бюджета'] / sdel_clean['Суммарная площадь сделок'].replace(0, np.nan)

# ============================================================
# 3. ФИЛЬТРАЦИЯ ВЫБРОСОВ
# ============================================================
mask_area_rooms_outlier = pd.Series(False, index=sdel_clean.index)
if 'Количество комнат' in sdel_clean.columns:
    unique_rooms = sdel_clean['Количество комнат'].unique()
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

Q1_p = sdel_clean['Price_m2'].quantile(0.25)
Q3_p = sdel_clean['Price_m2'].quantile(0.75)
IQR_p = Q3_p - Q1_p
mask_price_outlier = (sdel_clean['Price_m2'] < (Q1_p - 1.5*IQR_p)) | (sdel_clean['Price_m2'] > (Q3_p + 1.5*IQR_p))
mask_cheap = sdel_clean['Unit_Price'] < MIN_PRICE_UNIT

total_mask = mask_area_rooms_outlier | mask_price_outlier | mask_cheap 
sdel_final = sdel_clean[~total_mask].copy()

print(f"\n📊 Данные очищены. Удалено {total_mask.sum()} строк выбросов.")
print(f"   Осталось строк: {len(sdel_final)}")

# ============================================================
# 4. БЛОК НОВЫХ ГРАФИКОВ (КЛАССЫ + ИПОТЕКА)
# ============================================================
print("\n🎨 Построение графиков распределения...")

# --- ГРАФИК А: РАСПРЕДЕЛЕНИЕ СДЕЛОК ПО КЛАССАМ ---
# Используем sdel_final ДО фильтрации только на "Комфорт", чтобы видеть всё
if 'Класс' in sdel_final.columns:
    # Нормализация имен классов
    df_classes = sdel_final.copy()
    df_classes['Класс_Norm'] = df_classes['Класс'].astype(str).str.strip()
    
    # Группируем
    class_counts = df_classes.groupby('Класс_Norm')['Суммарное количество сделок'].sum().reset_index()
    class_counts = class_counts.sort_values('Суммарное количество сделок', ascending=False)
    
    plt.figure(figsize=(12, 6))
    ax = sns.barplot(data=class_counts, x='Класс_Norm', y='Суммарное количество сделок', palette='viridis')
    plt.title('Количество сделок по Классам жилья', fontsize=14)
    plt.xlabel('Класс проекта', fontsize=12)
    plt.ylabel('Количество сделок (шт)', fontsize=12)
    plt.xticks(rotation=45)
    
    # Подписи значений
    for p in ax.patches:
        ax.annotate(f'{int(p.get_height())}', 
                    (p.get_x() + p.get_width() / 2., p.get_height()), 
                    ha = 'center', va = 'center', 
                    xytext = (0, 9), textcoords = 'offset points')
    plt.tight_layout()
    plt.show()
else:
    print("⚠️ Колонка 'Класс' не найдена, график классов пропущен.")

# --- НОВЫЙ ГРАФИК: КЛАССЫ И ИПОТЕКА (СОВМЕСТНЫЙ) ---
print("\n📊 Построение графика 'Классы и Ипотека'...")

def plot_class_mortgage_distribution(df):
    """
    Строит график распределения сделок по классам с разбивкой по ипотеке
    """
    if 'Класс' not in df.columns or 'Ипотека' not in df.columns:
        print("⚠️ Нет колонок 'Класс' и/или 'Ипотека', график пропущен")
        return
    
    # Создаем копию данных
    df_viz = df.copy()
    
    # Нормализуем классы
    df_viz['Класс_Norm'] = df_viz['Класс'].astype(str).str.strip()
    
    # Нормализуем ипотеку
    mortgage_flags = ['да', 'yes', 'true', '1', 'ипотека']
    df_viz['Ипотека_Norm'] = df_viz['Ипотека'].astype(str).apply(
        lambda x: 'С ипотекой' if x.strip().lower() in mortgage_flags else 'Без ипотеки'
    )
    
    # Группируем по классам и ипотеке
    grouped = df_viz.groupby(['Класс_Norm', 'Ипотека_Norm'])['Суммарное количество сделок'].sum().reset_index()
    
    # Сортируем по общему количеству сделок
    class_totals = df_viz.groupby('Класс_Norm')['Суммарное количество сделок'].sum().reset_index()
    class_order = class_totals.sort_values('Суммарное количество сделок', ascending=False)['Класс_Norm'].tolist()
    
    # Фильтруем только топ-5 классов если их много
    if len(class_order) > 5:
        top_classes = class_order[:5]
        grouped = grouped[grouped['Класс_Norm'].isin(top_classes)]
        class_order = top_classes
    
    # Создаем график
    plt.figure(figsize=(14, 8))
    
    # Используем палитру для лучшей визуализации
    colors = ['#3B82F6', '#EF4444']  # Синий для ипотеки, Красный для без ипотеки
    color_dict = {'С ипотекой': colors[0], 'Без ипотеки': colors[1]}
    
    # Столбчатый график с группировкой
    ax = sns.barplot(
        data=grouped,
        x='Класс_Norm',
        y='Суммарное количество сделок',
        hue='Ипотека_Norm',
        palette=color_dict,
        order=class_order
    )
    
    plt.title('Распределение сделок по классам и ипотеке', fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Класс жилья', fontsize=14)
    plt.ylabel('Количество сделок (шт)', fontsize=14)
    plt.xticks(rotation=45)
    plt.legend(title='Тип сделки', title_fontsize=12)
    
    # Добавляем подписи значений
    for container in ax.containers:
        ax.bar_label(
            container,
            fmt='%.0f',
            label_type='edge',
            padding=3,
            fontsize=10
        )
    
    # Добавляем общее количество для каждого класса
    for i, (class_name, total) in enumerate(zip(class_order, class_totals.sort_values('Суммарное количество сделок', ascending=False)['Суммарное количество сделок'])):
        if class_name in grouped['Класс_Norm'].unique():
            plt.text(i, total + max(total*0.02, 100), f'Всего: {int(total)}', 
                    ha='center', va='bottom', fontweight='bold', fontsize=10,
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
    
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    plt.tight_layout()
    plt.show()
    
    # Статистика по комфорту
    comfort_mask = df_viz['Класс_Norm'].str.lower().str.contains('комфорт')
    if comfort_mask.any():
        comfort_data = df_viz[comfort_mask]
        comfort_total = comfort_data['Суммарное количество сделок'].sum()
        comfort_mortgage = comfort_data[comfort_data['Ипотека_Norm'] == 'С ипотекой']['Суммарное количество сделок'].sum()
        
        print(f"\n📈 СТАТИСТИКА ПО КОМФОРТУ:")
        print(f"   Всего сделок класса 'Комфорт': {comfort_total:,}")
        print(f"   Из них с ипотекой: {comfort_mortgage:,} ({comfort_mortgage/comfort_total*100:.1f}%)")
        print(f"   Без ипотеки: {comfort_total-comfort_mortgage:,} ({100 - comfort_mortgage/comfort_total*100:.1f}%)")

# Запускаем новый график
plot_class_mortgage_distribution(sdel_final)

# --- ГРАФИК Б: ИПОТЕКА И КОМНАТЫ (ТОЛЬКО КОМФОРТ) ---
# Теперь выделяем только Комфорт для детального графика
def is_comfort(val):
    s = str(val).lower()
    return 'комфорт' in s

if 'Класс' in sdel_final.columns:
    df_comfort = sdel_final[sdel_final['Класс'].apply(is_comfort)].copy()
else:
    df_comfort = sdel_final.copy() # Если колонки нет, берем всё

if not df_comfort.empty:
    # Подготовка данных по Ипотеке
    if 'Ипотека' not in df_comfort.columns:
        df_comfort['Ипотека'] = 'Нет'
    
    df_comfort['Ипотека'] = df_comfort['Ипотека'].fillna('Нет')
    # Унификация значений ипотеки
    mortgage_yes = ['да', 'yes', 'true', '1', 'ипотека']
    df_comfort['Mortgage_Clean'] = df_comfort['Ипотека'].astype(str).apply(
        lambda x: 'Да' if x.strip().lower() in mortgage_yes else 'Нет'
    )
    
    # Подготовка данных по Комнатам
    if 'Количество комнат' in df_comfort.columns:
        target_rooms = ['ст', '1', '2', '3', '4']
        df_rooms = df_comfort[df_comfort['Количество комнат'].astype(str).isin(target_rooms)].copy()
        
        # Сортировка комнат
        room_order_dict = {'ст': 0, '1': 1, '2': 2, '3': 3, '4': 4}
        grp_rooms = df_rooms.groupby('Количество комнат')['Суммарное количество сделок'].sum().reset_index()
        grp_rooms['sort_key'] = grp_rooms['Количество комнат'].map(room_order_dict)
        grp_rooms = grp_rooms.sort_values('sort_key')
    else:
        grp_rooms = pd.DataFrame()

    # Группировка по Ипотеке
    grp_mortgage = df_comfort.groupby('Mortgage_Clean')['Суммарное количество сделок'].sum().reset_index()

    # Рисуем двойной график
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.canvas.manager.set_window_title('Детализация сегмента КОМФОРТ')

    # 1. По комнатам
    if not grp_rooms.empty:
        bars1 = axes[0].bar(grp_rooms['Количество комнат'], grp_rooms['Суммарное количество сделок'], 
                           color='#5A9BD4', edgecolor='black')
        axes[0].set_title('Продажи по комнатам (Комфорт)', fontsize=14)
        axes[0].set_ylabel('Количество квартир', fontsize=12)
        axes[0].grid(axis='y', linestyle='--', alpha=0.5)
        for bar in bars1:
            axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height(), 
                         f'{int(bar.get_height())}', ha='center', va='bottom', fontweight='bold')
    
    # 2. По ипотеке
    bars2 = axes[1].bar(grp_mortgage['Mortgage_Clean'], grp_mortgage['Суммарное количество сделок'], 
                        color=['#FF9999', '#66B3FF'], edgecolor='black')
    axes[1].set_title('Сделки с ипотекой vs Без (Комфорт)', fontsize=14)
    axes[1].set_ylabel('Количество квартир', fontsize=12)
    axes[1].grid(axis='y', linestyle='--', alpha=0.5)
    for bar in bars2:
        axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height(), 
                     f'{int(bar.get_height())}', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.show()
else:
    print("⚠️ Нет данных класса 'Комфорт' для построения детальных графиков.")



# ============================================================
# 5. ГРАФИК ТОП-20 БАНКОВ ПО СДЕЛКАМ
# ============================================================
print("\n📊 Построение графика Топ-20 банков...")

def plot_top_mortgage_banks(df):
    """
    Строит график Топ-20 банков по количеству сделок на основе sdel_final.
    Стиль: Dark Theme (#13161C).
    """
    print("📊 Подготовка данных для графика банков...")
    
    # 1. Проверяем наличие колонок
    if "Название банка" not in df.columns:
        print("❌ Ошибка: Нет колонки 'Название банка'")
        return
        
    # 2. Фильтруем ипотеку (если есть колонка, иначе берем все)
    df_viz = df.copy()
    
    if "Ипотека" in df_viz.columns:
        # Приводим к нижнему регистру и ищем флаги "да", "yes", "1"
        mortgage_flags = ['да', 'yes', 'true', '1', 'ипотека']
        mask_mort = df_viz["Ипотека"].astype(str).str.lower().isin(mortgage_flags)
        df_viz = df_viz[mask_mort]
        print(f"   Отобрано ипотечных сделок: {len(df_viz)}")
    else:
        print("⚠️ Колонка 'Ипотека' не найдена, строим по всем сделкам.")

    # 3. Чистим названия банков
    df_viz["Название банка"] = df_viz["Название банка"].fillna("Не указан").astype(str).str.strip()
    # Убираем мусор
    bad_names = ["nan", "None", "Не указан", "0", "Рассрочка", "Нет"]
    df_viz = df_viz[~df_viz["Название банка"].isin(bad_names)]

    # 4. Считаем Топ-20
    top_banks = df_viz["Название банка"].value_counts().head(20).reset_index()
    top_banks.columns = ["Bank", "Count"]

    if top_banks.empty:
        print("⚠️ Нечего рисовать (пустой список банков).")
        return

    # --- НАСТРОЙКИ ТЕМНОЙ ТЕМЫ ---
    dark_bg = '#13161C'
    text_color = '#FFFFFF'
    grid_color = '#3A4250'
    
    plt.rcParams.update({
        "figure.facecolor": dark_bg,
        "axes.facecolor": dark_bg,
        "axes.edgecolor": dark_bg,
        "axes.labelcolor": text_color,
        "xtick.color": text_color,
        "ytick.color": text_color,
        "text.color": text_color,
        "grid.color": grid_color,
    })

    # --- РИСОВАНИЕ ---
    plt.figure(figsize=(16, 10))

    # Градиент синего от светлого к темному
    ax = sns.barplot(
        data=top_banks,
        y="Bank",
        x="Count",
        palette=sns.color_palette("Blues", n_colors=20),  # Синяя палитра
        edgecolor=None
    )

    # Добавляем цифры справа
    for container in ax.containers:
        ax.bar_label(
            container, 
            fmt='%.0f', 
            label_type='edge', 
            padding=10, 
            color='white', 
            fontsize=11, 
            fontweight='bold'
        )

    # Косметика
    plt.title("Топ-20 банков по количеству сделок", fontsize=20, pad=20, color='white')
    plt.xlabel("Количество сделок", fontsize=12, labelpad=15)
    plt.ylabel("", fontsize=12)
    
    # Сетка и рамки
    ax.xaxis.grid(True, linestyle='-', alpha=0.3, color=grid_color)
    ax.yaxis.grid(False)
    sns.despine(left=True, bottom=False)
    
    plt.tight_layout()
    plt.show()
    
    # Сброс настроек (чтобы не сломать другие графики)
    plt.rcdefaults()

# Запуск функции построения графика банков
plot_top_mortgage_banks(sdel_final)

# ============================================================
# 6. ML PREPROCESSING (ПЕРЕСБОРКА ДЛЯ АНАЛИЗА БАНКОВ)
# ============================================================

def process_real_estate_data_ml(proj_df, deals_df, bank_percentile=(0, 100)):
    print("\n🚀 ЗАПУСК ML-ПРЕПРОЦЕССИНГА (ТОЛЬКО КОМФОРТ + ИПОТЕЧНЫЙ SELLOUT)")
    
    # 1. Фильтр классов (Strict Comfort) - КАК ВО ВТОРОМ КОДЕ
    def norm_class_to_key(x):
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
    
    # Фильтруем проекты
    if 'Класс проекта' in proj_df.columns:
        proj_df['_class_norm'] = proj_df['Класс проекта'].apply(norm_class_to_key)
        proj_df = proj_df[proj_df['_class_norm'] == 'комфорт'].copy()
        proj_df.drop(columns=['_class_norm'], inplace=True)
        print(f"   Строк проектов (Комфорт после строгой фильтрации): {len(proj_df)}")
    else:
        print("⚠️ Колонка 'Класс проекта' не найдена в proj_df")
    
    # Фильтруем сделки - ВАЖНО: как во втором коде используем INNER JOIN
    if 'Класс' in deals_df.columns:
        deals_df['_class_norm'] = deals_df['Класс'].apply(norm_class_to_key)
        deals_df = deals_df[deals_df['_class_norm'] == 'комфорт'].copy()
        deals_df.drop(columns=['_class_norm'], inplace=True)
    
    # Проверяем наличие необходимых колонок
    if 'ID корпуса' not in proj_df.columns or 'ID корпуса' not in deals_df.columns:
        print("❌ Ошибка: Нет колонки 'ID корпуса' в одной из таблиц")
        return pd.DataFrame()
    
    # Подготовка названий проектов в proj_df
    if 'Проект' not in proj_df.columns:
        # Ищем альтернативные названия
        for col in ['Название ЖК', 'ЖК']:
            if col in proj_df.columns:
                proj_df.rename(columns={col: 'Проект'}, inplace=True)
                print(f"   Переименовано '{col}' в 'Проект' в proj_df")
                break
    
    # Подготовка названий проектов в deals_df
    if 'Проект' not in deals_df.columns:
        for col in ['Название ЖК', 'ЖК']:
            if col in deals_df.columns:
                deals_df.rename(columns={col: 'Проект'}, inplace=True)
                print(f"   Переименовано '{col}' в 'Проект' в deals_df")
                break
    
    # КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ: Используем INNER JOIN как во втором коде
    print("   Используем INNER JOIN для связи проектов и сделок по ID корпуса")
    
    # Создаем маппинг корпус-проект из proj_df
    corpus_map = proj_df[['ID корпуса', 'Проект']].drop_duplicates().set_index('ID корпуса')
    n_before_deals = len(deals_df)
    
    # Выполняем INNER JOIN
    deals_df = deals_df.merge(corpus_map, on='ID корпуса', how='inner')  # INNER JOIN!
    
    print(f"   deals после INNER мёржа по корпусам: было {n_before_deals}, стало {len(deals_df)}")
    
    # ВАЖНО: После merge проверяем, какая колонка Проект появилась
    # После merge могут появиться колонки 'Проект_x' и 'Проект_y'
    # Нам нужна колонка из proj_df (обычно 'Проект_y')
    
    # Определяем, какая колонка с названием проекта у нас есть
    project_col_in_deals = None
    for col in ['Проект_y', 'Проект_x', 'Проект']:
        if col in deals_df.columns:
            project_col_in_deals = col
            print(f"   Найдена колонка проекта в deals_df: '{project_col_in_deals}'")
            break
    
    if not project_col_in_deals:
        print("❌ Ошибка: После merge в deals_df нет колонки с названием проекта")
        print("   Доступные колонки в deals_df:", list(deals_df.columns))
        return pd.DataFrame()
    
    # Переименовываем в единое имя 'Проект'
    deals_df = deals_df.rename(columns={project_col_in_deals: 'Проект'})
    
    # Удаляем лишние колонки если они есть
    for col in ['Проект_x', 'Проект_y']:
        if col in deals_df.columns and col != 'Проект':
            deals_df.drop(columns=[col], inplace=True)
        
    # 2. Фильтр банков (для фичей)
    deals_df['Название банка'] = deals_df['Название банка'].fillna('Не указан').astype(str).str.strip()
    bank_stats = deals_df.groupby('Название банка')['Суммарная площадь сделок'].sum().reset_index()
    low = np.percentile(bank_stats['Суммарная площадь сделок'], bank_percentile[0])
    high = np.percentile(bank_stats['Суммарная площадь сделок'], bank_percentile[1])
    selected_banks = bank_stats[
        (bank_stats['Суммарная площадь сделок'] >= low) & 
        (bank_stats['Суммарная площадь сделок'] <= high)
    ]['Название банка'].tolist()
    
    # Веса банков
    bank_weights = bank_stats.set_index('Название банка')['Суммарная площадь сделок'].apply(np.log1p).to_dict()

    # 3. Подготовка дат и объединение
    # Гарантируем колонку dt_deal
    if 'Дата договора (месяц.год)' in deals_df.columns:
        deals_df['dt_deal'] = pd.to_datetime(deals_df['Дата договора (месяц.год)'], dayfirst=True, errors='coerce')
    elif 'Дата регистрации (месяц.год)' in deals_df.columns:
        deals_df['dt_deal'] = pd.to_datetime(deals_df['Дата регистрации (месяц.год)'], dayfirst=True, errors='coerce')
        print("   Используем 'Дата регистрации (месяц.год)' как дату сделки")
    else:
        print("❌ Ошибка: Нет колонки с датой сделки")
        return pd.DataFrame()
    
    deals_df = deals_df.dropna(subset=['dt_deal'])

    # Привязываем дату старта к проектам (как во втором коде)
    corpus_start = deals_df.groupby('ID корпуса')['dt_deal'].min().reset_index().rename(columns={'dt_deal': 'corpus_start'})
    proj_df = proj_df.merge(corpus_start, on='ID корпуса', how='left')
    
    # Агрегируем старт проекта
    proj_start = proj_df.groupby('Проект')['corpus_start'].min().reset_index().rename(columns={'corpus_start': 'project_start'})
    proj_df = proj_df.merge(proj_start, on='Проект', how='left')
    
    # Список проектов
    projects = proj_df['Проект'].unique()
    mortgage_flags = ['да', 'yes', 'true', '1', 'ипотека']
    
    data_list = []
    
    # КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ: Проверка временных рамок как во втором коде
    MAX_DATE = deals_df['dt_deal'].max()
    
    for proj_name in projects:
        # Данные по проекту
        proj_subset = proj_df[proj_df['Проект'] == proj_name]
        if proj_subset.empty: 
            continue
        
        t0 = proj_subset['project_start'].min()
        if pd.isna(t0): 
            continue
        
        # Проверка временных рамок - КАК ВО ВТОРОМ КОДЕ
        if (MAX_DATE - t0).days < 365:
            continue  # Пропускаем проекты с недостаточным временным периодом
        
        # --- ГОД 1 (0-365 дней) ---
        t_end = t0 + timedelta(days=365)
        
        # Знаменатель: Общая площадь корпусов, вышедших в продажу в 1-й год
        valid_corps = proj_subset[proj_subset['corpus_start'] <= t_end]
        planned_area = valid_corps['Общая проектная площадь'].sum()
        planned_lots = valid_corps['Количество лотов'].sum()
        
        if planned_area <= 0: 
            continue
        
        # Числитель: Ипотечные сделки за 1-й год
        # ИСПРАВЛЕНО: используем deals_df['Проект']
        mask_deals = (deals_df['Проект'] == proj_name) & \
                     (deals_df['dt_deal'] >= t0) & \
                     (deals_df['dt_deal'] <= t_end)
        
        deals_subset = deals_df[mask_deals]
        
        # Фильтр ипотеки
        mort_deals = deals_subset[deals_subset['Ипотека'].astype(str).str.lower().isin(mortgage_flags)]
        
        sold_area = mort_deals['Суммарная площадь сделок'].sum()
        deals_count = mort_deals['Суммарное количество сделок'].sum()
        
        # Фичи банков
        proj_banks = mort_deals[mort_deals['Название банка'].isin(selected_banks)]['Название банка'].unique()
        num_banks = len(proj_banks)
        bank_idx = sum(bank_weights.get(b, 0) for b in proj_banks)
        
        # Sellout
        sellout = (sold_area / planned_area) * 100 if planned_area > 0 else 0
        
        if sellout > 100: 
            continue # Отсекаем явные ошибки данных
        
        # Ищем колонку округа
        district_col = None
        for col in ['Округ', 'Район', 'Административный округ']:
            if col in deals_subset.columns and not deals_subset[col].mode().empty:
                district_col = col
                break
        
        district = deals_subset[district_col].mode()[0] if district_col else None
        
        data_list.append({
            'Проект': proj_name,
            'sellout_12m': sellout,
            'deals_count_12m': deals_count,
            'sold_area_12m': sold_area,
            'planned_area': planned_area,
            'planned_lots': planned_lots,
            'num_banks_12m': num_banks,
            'bank_index': bank_idx,
            'Округ': district
        })
        
    return pd.DataFrame(data_list)

# Запуск ML препроцессинга
df_ml = process_real_estate_data_ml(proj_clean, sdel_final)

if df_ml.empty:
    print("⛔ ML-датасет пуст. Проверьте фильтры или данные.")
    
    # Дополнительная диагностика: покажем доступные колонки
    print("\n📋 Доступные колонки в proj_clean:")
    print([col for col in proj_clean.columns if 'проект' in col.lower() or 'жк' in col.lower()])
    
    print("\n📋 Доступные колонки в sdel_final:")
    print([col for col in sdel_final.columns if 'проект' in col.lower() or 'жк' in col.lower() or 'сделка' in col.lower()])
else:
    # Добавление логарифмов
    df_ml['log_planned_area'] = np.log1p(df_ml['planned_area'])
    df_ml['log_deals_12m'] = np.log1p(df_ml['deals_count_12m'])

    print(f"\n✅ ML-датасет готов: {len(df_ml)} строк (проектов).")
    print(df_ml.head())