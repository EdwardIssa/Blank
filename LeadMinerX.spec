# -*- mode: python ; coding: utf-8 -*-
#
# LeadMinerX.spec — PyInstaller build spec for LeadMiner X v3.
#
# Build locally with:  pyinstaller LeadMinerX.spec
# Output:              dist/LeadMinerX.exe   (Windows)
#                      dist/LeadMinerX       (macOS, onefile binary)
#
# Notes:
#   * onefile + windowed: a single double-clickable executable with no
#     console window — the right shape for a customer-facing desktop tool.
#   * customtkinter loads its theme/asset files from disk at runtime.
#     collect_data_files('customtkinter') bundles them into the exe;
#     without it the frozen app crashes on launch with a missing-theme
#     error even though the .py runs fine from source.

from PyInstaller.utils.hooks import collect_data_files

# Bundle CustomTkinter's theme JSONs and image assets.
ctk_datas = collect_data_files('customtkinter')

a = Analysis(
    ['leadminer_x.py'],
    pathex=[],
    binaries=[],
    datas=ctk_datas,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='LeadMinerX',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,          # windowed: no terminal window behind the GUI
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
