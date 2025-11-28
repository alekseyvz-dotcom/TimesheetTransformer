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
    datas=pandas_datas + psycopg2_datas,
    hiddenimports=[
        'settings_manager', 'meals_module', 'SpecialOrders', 'objects',
        'assets_logo', 'timesheet_transformer', 'BudgetAnalyzer',
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
    clean=True  # <--- ВАЖНО: Добавлена эта строка
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [], # Эта пустая коллекция нужна для --onefile
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
