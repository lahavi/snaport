@echo off
rem Mounts a raw disk image read-only inside WSL2 so its files can be
rem browsed from Windows Explorer via \\wsl.localhost\<distro>\<mount>.
rem Handles GPT/MBR partitioned images (mounts partition 1).
rem
rem Usage:   mount-image.cmd C:\path\to\image.img [wsl-mount-point]
rem Example: mount-image.cmd C:\dev\snaport\snap-0b4d0ed03fd9b7795.img
rem
rem Unmount when done:
rem   wsl -u root umount /mnt/snaport
rem   wsl -u root losetup -D

setlocal EnableExtensions
if "%~1"=="" (
  echo Usage: mount-image.cmd ^<path\to\image.img^> [wsl-mount-point]
  echo Example: mount-image.cmd C:\dev\snaport\snap-0b4d0ed03fd9b7795.img
  exit /b 1
)

set "MNT=/mnt/snaport"
if not "%~2"=="" set "MNT=%~2"

rem Translate the Windows path to a WSL path (wslpath prints it directly).
set "WSLIMG="
for /f "usebackq delims=" %%i in (`wsl wslpath -u "%~1"`) do set "WSLIMG=%%i"
if not defined WSLIMG (
  echo error: could not translate "%~1" into a WSL path - is WSL installed?
  exit /b 1
)

wsl -u root mkdir -p "%MNT%" || goto :fail

rem Attach the image and capture the assigned loop device.
set "LOOP="
for /f "usebackq delims=" %%i in (`wsl -u root losetup -fP --show "%WSLIMG%"`) do set "LOOP=%%i"
if not defined LOOP (
  echo error: could not attach "%WSLIMG%" to a loop device
  goto :fail
)

rem Mount read-only; norecovery is the fallback for XFS images that were
rem snapshotted while mounted (needs log replay).
wsl -u root mount -o ro "%LOOP%p1" "%MNT%" 2>nul
if errorlevel 1 (
  wsl -u root mount -o ro,norecovery "%LOOP%p1" "%MNT%" || goto :fail
)

echo Mounted %LOOP%p1 read-only at %MNT%. Top-level contents:
wsl -u root ls "%MNT%"

echo.
echo Browse the files in Windows Explorer at:
echo   \\wsl.localhost\Ubuntu%MNT%
echo   (or \\wsl$\Ubuntu%MNT%)
echo.
echo The mount disappears when the WSL VM stops. Re-run this script to
echo mount again. Unmount with:
echo   wsl -u root umount %MNT% ^&^& wsl -u root losetup -D
exit /b 0

:fail
echo error: mount failed - detach any partial attach with: wsl -u root losetup -D
exit /b 1
