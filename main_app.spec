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
        # --- Наши модули ---
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
        ('estimate_resource_decoder.py', '.'),
        ('timesheet_module.py', '.'),
        ('timesheet_db.py', '.'),
        ('timesheet_common.py', '.'),
        ('timesheet_dialogs.py', '.'),
        ('work_schedules_manager.py', '.'),
        ('employee_card.py', '.'),
        ('analytics_module.py', '.'),
        ('timesheet_plan_fact_page.py', '.'),
        ('payroll_module.py', '.'),
        ('brigades_module.py', '.'),
        ('gpr_module.py', '.'),
        ('gpr_dictionaries.py', '.'),
        ('gpr_task_dialog.py', '.'),
        ('trip_timesheet_page.py', '.'),
        ('trip_timesheet_db.py', '.'),
        ('trip_period_dialog.py', '.'),
    ] + pandas_datas + psycopg2_datas,
    hiddenimports=[
        # --- Наши модули ---
        'settings_manager',
        'menu_spec',
        'analytics_module',
        'timesheet_plan_fact_page',
        'meals_module',
        'meals_employees',
        'SpecialOrders',
        'lodging_module',
        'objects',
        'assets_logo',
        'timesheet_transformer',
        'timesheet_compare',
        'virtual_timesheet_grid',
        'timesheet_module',
        'timesheet_db',
        'timesheet_common',
        'timesheet_dialogs',
        'work_schedules_manager',
        'employees',
        'BudgetAnalyzer',
        'estimate_resource_decoder',
        'employee_card',
        'payroll_module',
        'brigades_module',
        'gpr_module',
        'gpr_dictionaries',
        'gpr_task_dialog',
        'trip_timesheet_page',
        'trip_timesheet_db',
        'trip_period_dialog',

        # --- Библиотеки ---
        'psycopg2',
        'psycopg2.extras',

        'pandas',
        'openpyxl',
        'PIL',
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
