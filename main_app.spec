# main_app.spec

# -*- mode: python ; coding: utf-8 -*-
import os
from PyInstaller.utils.hooks import collect_submodules, collect_data_files

block_cipher = None
spec_dir = os.getcwd()

# --- Сбор данных для сложных библиотек ---
pandas_datas = collect_data_files('pandas')
psycopg2_datas = collect_data_files('psycopg2')

a = Analysis(
    ['main_app.py'],
    pathex=[spec_dir],
    binaries=[],
    datas=[
        # --- ВАЖНО: Явно добавляем все наши модули как файлы данных ---
        ('settings_manager.py', '.'),
        ('menu_spec.py', '.'),
        ('meals_module.py', '.'),
        ('meals_employees.py', '.'),
        ('SpecialOrders.py', '.'),
        ('lodging_module.py', '.'),
        ('objects.py', '.'),
        ('assets_logo.py', '.'),
        ('timesheet_transformer.py', '.'),
        ('virtual_timesheet_grid.py', '.'),
        ('timesheet_compare.py', '.'),
        ('employees.py', '.'),
        ('BudgetAnalyzer.py', '.'),
        ('timesheet_module.py', '.'),
        ('employee_card.py', '.'),
        ('analytics_module.py', '.'),
        ('payroll_module.py', '.'),
        ('brigades_module.py', '.'),
    ] + pandas_datas + psycopg2_datas, # Добавляем данные библиотек
    hiddenimports=[
        'settings_manager', 'menu_spec.py', 'analytics_module.py', 'meals_module', 'meals_employees.py', 'SpecialOrders', 'lodging_module.py', 'objects',
        'assets_logo', 'timesheet_transformer', 'timesheet_compare.py', 'virtual_timesheet_grid.py', 'employees.py', 'BudgetAnalyzer', 'timesheet_modyle', 'employee_card.py', 'payroll_module', 'brigades_module',
        'psycopg2', 'psycopg2.extras',
        'pandas', 'openpyxl', 'PIL'
    ] + collect_submodules('pandas'),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
    clean=True
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='TabelSuite_Unified_Package_D_Mode',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='icon.ico'
)
