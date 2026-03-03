# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for MEP Oracle Gatherer
# Builds a single portable executable (Windows .exe or Linux binary)
#
# BUILD (Windows):
#   pip install pyinstaller
#   pyinstaller mep_oracle_launcher.spec
#
# BUILD (Linux):
#   pip install pyinstaller
#   pyinstaller mep_oracle_launcher.spec
#
# Output: dist/MEP_Oracle_Gatherer.exe (Windows) or dist/MEP_Oracle_Gatherer (Linux)

import sys

block_cipher = None

a = Analysis(
    ['mep_oracle_launcher.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
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

exe_kwargs = dict(
    name='MEP_Oracle_Gatherer',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

# No UAC elevation needed for Oracle (unlike SQL Server)
# sqlplus doesn't require admin privileges

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    **exe_kwargs,
)
