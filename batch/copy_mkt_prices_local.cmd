@echo off
REM Define source and destination paths
@REM set "source=\\rga.net\stlfinmkts\MarketRiskMgmt\Pricing Requests\2024-Orion - IUL Hedging\RGA_Process\1_Code\MktData\OrionWinterfell_Index_Prices.csv"
set "source=G:\MarketRiskMgmt\Pricing Requests\2024-Orion - IUL Hedging\RGA_Process\1_Code\MktData\OrionWinterfell_Index_Prices.csv"
set "destination=..\MktData\OrionWinterfell_Index_Prices.csv"

REM Debugging: Print paths
echo Source: "%source%"
echo Destination: "%destination%"



REM Copy the file and overwrite if it exists
echo Copying file from "%source%" to "%destination%"...
copy /Y "%source%" "%destination%"

REM Check if the copy was successful
if %errorlevel% equ 0 (
    echo File copied successfully.
) else (
    echo Failed to copy the file.
)

pause