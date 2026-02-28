"""
consolidar_boletos.py
---------------------
Procesa todos los archivos HTML de boletos VETA/ByMA contenidos en una carpeta
y consolida los resúmenes de cada uno en un único DataFrame.

Requiere que parse_boleto.py esté en la misma carpeta que este script.

Uso desde la terminal de Windows:
    python consolidar_boletos.py "C:\\Users\\Fernando\\Documentos\\Boletos"

Uso desde Python (importando la función):
    from consolidar_boletos import consolidar_carpeta
    df = consolidar_carpeta("C:/Users/Fernando/Documentos/Boletos")

Salida:
    - Imprime el DataFrame consolidado en pantalla.
    - Guarda un Excel "resumen_consolidado.xlsx" dentro de la misma carpeta.
"""

import sys
from pathlib import Path

import pandas as pd

# Importar la función de parseo del script anterior
from parse_boleto import parse_boleto


def consolidar_carpeta(carpeta: str, extensiones=(".htm", ".html")) -> pd.DataFrame:
    """
    Lee todos los archivos HTML de `carpeta` y fusiona los DataFrames de
    resumen en uno solo.

    Parámetros
    ----------
    carpeta     : str o Path — ruta a la carpeta con los archivos HTML.
    extensiones : tupla de extensiones a buscar (insensible a mayúsculas).

    Retorna
    -------
    pd.DataFrame con el resumen consolidado de todos los boletos,
    ordenado por Fecha Concertación + Especie + Operación.
    Incluye la columna "Archivo" para identificar el origen de cada fila.
    """
    carpeta = Path(carpeta)

    if not carpeta.exists():
        raise FileNotFoundError(f"La carpeta no existe: {carpeta}")
    if not carpeta.is_dir():
        raise NotADirectoryError(f"La ruta no es una carpeta: {carpeta}")

    # Buscar archivos HTML (case-insensitive en la extensión)
    archivos = [
        f for f in sorted(carpeta.iterdir())
        if f.is_file() and f.suffix.lower() in extensiones
    ]

    if not archivos:
        print(f"⚠  No se encontraron archivos HTML en: {carpeta}")
        return pd.DataFrame()

    print(f"📂  Carpeta: {carpeta}")
    print(f"📄  Archivos encontrados: {len(archivos)}\n")

    resumenes = []
    errores   = []

    for archivo in archivos:
        try:
            _, df_res = parse_boleto(str(archivo))

            if df_res.empty:
                print(f"  ⚠  Sin datos: {archivo.name}")
                continue

            # Agregar columna con el nombre del archivo origen
            df_res.insert(0, "Archivo", archivo.name)
            resumenes.append(df_res)
            print(f"  ✅  {archivo.name:45s} → {len(df_res)} fila(s)")

        except Exception as e:
            errores.append(archivo.name)
            print(f"  ❌  {archivo.name:45s} → ERROR: {e}")

    if not resumenes:
        print("\n⚠  No se pudo procesar ningún archivo.")
        return pd.DataFrame()

    # Consolidar
    df_consolidado = pd.concat(resumenes, ignore_index=True)

    # Ordenar cronológicamente
    df_consolidado.sort_values(
        by=["Fecha Concertación", "Especie", "Operación"],
        inplace=True,
        ignore_index=True,
    )

    if errores:
        print(f"\n⚠  Archivos con error ({len(errores)}): {', '.join(errores)}")

    print(f"\n📊  Total de filas consolidadas: {len(df_consolidado)}")
    return df_consolidado


def guardar_excel(df: pd.DataFrame, carpeta: str, nombre: str = "resumen_consolidado.xlsx") -> Path:
    """Guarda el DataFrame consolidado en un Excel dentro de la carpeta."""
    out_path = Path(carpeta) / nombre
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Resumen Consolidado", index=False)

        # Autoajustar ancho de columnas
        ws = writer.sheets["Resumen Consolidado"]
        for col_cells in ws.columns:
            max_len = max(
                len(str(cell.value)) if cell.value is not None else 0
                for cell in col_cells
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)

    return out_path


# ── Ejecución directa ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python consolidar_boletos.py <ruta_carpeta>")
        print('Ej:  python consolidar_boletos.py "C:\\Boletos\\2026"')
        sys.exit(1)

    carpeta_arg = sys.argv[1]

    df = consolidar_carpeta(carpeta_arg)

    if df.empty:
        sys.exit(0)

    # Mostrar resultado en pantalla
    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", 260)
    pd.set_option("display.float_format", "{:,.2f}".format)

    print("\n" + "=" * 80)
    print("RESUMEN CONSOLIDADO")
    print("=" * 80)
    print(df.to_string(index=False))

    # Guardar Excel
    out = guardar_excel(df, carpeta_arg)
    print(f"\n💾  Excel guardado en: {out}")
