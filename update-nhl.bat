@echo off
echo =============================================
echo  NHL Money Shots - Auto Updater
echo =============================================
echo.

REM Find the most recently downloaded nhl-shots-main file
set "latest="
for /f "delims=" %%i in ('dir /b /o-d "C:\Users\Higgi\Downloads\nhl-shots-main*.py" 2^>nul') do (
  if not defined latest set "latest=%%i"
)

if not defined latest (
  echo ERROR: No nhl-shots-main*.py found in Downloads folder.
  echo Please download the file from the chat first.
  pause
  exit /b 1
)

echo Found: %latest%
echo Copying to nhl-shots-app folder...
copy "C:\Users\Higgi\Downloads\%latest%" "C:\Users\Higgi\nhl-shots-app\main.py" /y

echo.
echo Pushing to GitHub...
cd C:\Users\Higgi\nhl-shots-app
git add .
git commit -m "update nhl money shots"
git push origin main --force

echo.
echo =============================================
echo  DONE! Render will auto-deploy in ~2 mins
echo =============================================
pause
