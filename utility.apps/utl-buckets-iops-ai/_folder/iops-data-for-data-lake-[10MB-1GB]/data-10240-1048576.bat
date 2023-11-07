@echo off
setlocal enabledelayedexpansion

rem Obtiene el nombre del archivo .bat sin la extensión
for %%F in ("%~nx0") do (
    set "nombre_archivo=%%~nF"
)

rem Divide el nombre del archivo en tokens usando guiones como delimitadores
for /f "tokens=1,2,3 delims=-" %%A in ("!nombre_archivo!") do (
    set "nombre_base=%%A"
    set "tamanio_minimo_kb=%%B"
    set "tamanio_maximo_kb=%%C"
)

rem Obtiene la ubicación actual del archivo .bat
set "carpeta_destino=%~dp0"

rem Crea los archivos con tamaños dentro del rango especificado
for /l %%x in (1, 1, 10) do (
  set /a "tamanio_bytes=!random! %% (!tamanio_maximo_kb! - !tamanio_minimo_kb! + 1) + !tamanio_minimo_kb!"
  fsutil file createnew "!carpeta_destino!\!nombre_base!%%x.txt" !tamanio_bytes!000
)

echo Archivos creados exitosamente en la ubicación: %carpeta_destino%
pause