# main_app.spec

# -*- mode: python ; coding: utf-8 -*-
import os
block_cipher = None
spec_dir = os.path.dirname(os.path.abspath(__file__))

a = Analysis(
    ['main_app.py'],
    pathex=[spec_dir],  # Явно указываем, где искать наши .py файлы
    binaries=[],
    datas=[],
    hiddenimports=[
        # Перечисляем ВСЕ наши модули
        'settings_manager',
        'meals_module',
        'SpecialOrders',
        'objects',
        'assets_logo',
        'timesheet_transformer',
        'BudgetAnalyzer',
        
        # Явно указываем библиотеки, которые PyInstaller может "потерять"
        'psycopg2',
        'pandas',
        'openpyxl',
        'PIL'
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='TabelSuite_Unified_Package_D_Mode', # Имя вашего будущего exe-файла
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    runtime_tmpdir=None,
    console=False, # Эквивалент --windowed или --noconsole
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='icon.ico' # Путь к вашей иконке
)
