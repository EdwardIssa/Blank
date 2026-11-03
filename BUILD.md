# Building LeadMiner X v3

LeadMiner X ships as a single-file executable built with PyInstaller.
Buyers get one file — no Python install, no setup steps.

## How the pipeline works

1. The source (`leadminer_x.py`, `LeadMinerX.spec`, `requirements.txt`)
   lives in the **EdwardIssa/Blank** repo.
2. Every push to `main` or `master` (and every `v*` tag) triggers the
   **Build LeadMinerX** workflow in GitHub Actions
   (`.github/workflows/build-windows.yml`).
3. It builds on Windows and macOS runners in parallel and uploads the
   results as artifacts.
4. To get the Windows build: open the repo → **Actions** tab → latest
   **Build LeadMinerX** run → download the **LeadMinerX-windows**
   artifact → unzip → `LeadMinerX.exe`.

You can also start a build by hand: Actions tab → Build LeadMinerX →
**Run workflow**.

## Cutting a customer release

1. Tag the commit: `git tag v3.0.0 && git push origin v3.0.0`
2. The workflow builds as usual, and because the run came from a tag it
   also creates a GitHub **Release** for `v3.0.0` with `LeadMinerX.exe`
   attached.
3. Send customers the release download link for the `.exe` asset.

## Local builds

Requires Python 3.12.

**Windows**

```bat
pip install pyinstaller -r requirements.txt
pyinstaller LeadMinerX.spec
:: Output: dist\LeadMinerX.exe
```

**macOS**

```bash
pip install pyinstaller -r requirements.txt
pyinstaller LeadMinerX.spec
# Output: dist/LeadMinerX
```

## Unsigned executable — SmartScreen warning

The `.exe` is not code-signed, so on first run Windows SmartScreen will
show a "Windows protected your PC" warning. This is expected for a new
unsigned app and goes away once the file builds download reputation.

FAQ sentence for customers:

> When you first open LeadMinerX, Windows may show a blue "Windows protected your PC" box — click **More info**, then **Run anyway**; that's the standard prompt for a new app that isn't yet widely downloaded, and LeadMinerX will open normally after that.
