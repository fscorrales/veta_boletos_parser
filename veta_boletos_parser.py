#!/usr/bin/env python3
"""
Author : Fernando Corrales <fscpython@gmail.com>
Date   : 28-feb-2026
Purpose: 
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

__all__ = ["VetaBoletosParser"]

import sys
import argparse
import asyncio
import inspect
import os
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup


# --------------------------------------------------
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

# --------------------------------------------------
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

# --------------------------------------------------
def to_float(text):
    if not text:
        return None
    cleaned = text.strip().lstrip("$").replace(",", "").replace("*", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None

# --------------------------------------------------
def cell_text(cells, idx):
    try:
        return cells[idx].get_text(strip=True)
    except IndexError:
        return ""

# --------------------------------------------------
def th_names(table):
    return [th.get_text(strip=True) for th in table.find_all("th")]


CAB_REQUIRED = {"Fecha Concertación", "Especie", "Operación"}
DET_REQUIRED = {"Cantidad", "Precio", "Importe Bruto", "Detalle", "Gasto"}

# --------------------------------------------------
def parse_boleto(filepath):
    html = Path(filepath).read_text(encoding="utf-8-sig")
    soup = BeautifulSoup(html, "html.parser")

    cab_tables = []
    det_tables = []

    for t in soup.find_all("table"):
        hs = th_names(t)
        hs_set = set(hs)
        if len(hs) == 5 and CAB_REQUIRED.issubset(hs_set):
            cab_tables.append(t)
        elif len(hs) == 11 and DET_REQUIRED.issubset(hs_set):
            det_tables.append(t)

    if len(cab_tables) != len(det_tables):
        print(f"⚠  {len(cab_tables)} tablas cabecera / {len(det_tables)} tablas detalle")

    n = min(len(cab_tables), len(det_tables))
    operaciones_rows = []
    resumen_rows = []

    for cab_t, det_t in zip(cab_tables[:n], det_tables[:n]):

        cab_hs = th_names(cab_t)
        cab_data = [r for r in cab_t.find_all("tr") if not r.find("th")]
        if not cab_data:
            continue
        cells_cab = cab_data[0].find_all("td")

        def get_cab(col):
            try:
                return cells_cab[cab_hs.index(col)].get_text(strip=True)
            except (ValueError, IndexError):
                return ""

        fecha_conc = get_cab("Fecha Concertación")
        fecha_liq  = get_cab("Fecha Liquidación")
        operacion  = get_cab("Operación").strip()
        especie    = get_cab("Especie")

        det_hs = th_names(det_t)
        idx_cant    = det_hs.index("Cantidad")      if "Cantidad"      in det_hs else 0
        idx_precio  = det_hs.index("Precio")        if "Precio"        in det_hs else 1
        idx_bruto   = det_hs.index("Importe Bruto") if "Importe Bruto" in det_hs else 2
        idx_detalle = det_hs.index("Detalle")       if "Detalle"       in det_hs else 8
        idx_gasto   = det_hs.index("Gasto")         if "Gasto"         in det_hs else 10

        arancel = d_mercado = importe_neto = None
        ops_this_block = []

        det_data = [r for r in det_t.find_all("tr") if not r.find("th")]
        for row in det_data:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            cant_raw   = cell_text(cells, idx_cant)
            precio_raw = cell_text(cells, idx_precio)
            bruto_raw  = cell_text(cells, idx_bruto)
            detalle    = cell_text(cells, idx_detalle).upper().strip()
            gasto_raw  = cell_text(cells, idx_gasto)

            if detalle == "ARANCEL":
                arancel = to_float(gasto_raw)
            elif detalle == "D.MERCADO":
                d_mercado = to_float(gasto_raw)
            elif detalle == "IMPORTE NETO":
                importe_neto = to_float(gasto_raw)

            cant_val   = to_float(cant_raw)
            precio_val = to_float(precio_raw)
            bruto_val  = to_float(bruto_raw)

            if (
                cant_val is not None
                and "*" not in cant_raw
                and precio_val is not None
                and bruto_val is not None
            ):
                row_dict = {
                    "Fecha Concertación": fecha_conc,
                    "Fecha Liquidación":  fecha_liq,
                    "Operación":          operacion,
                    "Especie":            especie,
                    "Cantidad":           cant_val,
                    "Precio":             precio_val,
                    "Importe Bruto":      bruto_val,
                }
                operaciones_rows.append(row_dict)
                ops_this_block.append(row_dict)

        total_cant  = sum(r["Cantidad"]      for r in ops_this_block)
        total_bruto = sum(r["Importe Bruto"] for r in ops_this_block)
        precio_prom = round(total_bruto / total_cant, 6) if total_cant else None

        resumen_rows.append({
            "Fecha Concertación":  fecha_conc,
            "Fecha Liquidación":   fecha_liq,
            "Operación":           operacion,
            "Especie":             especie,
            "Cantidad Total":      total_cant,
            "Precio Prom. Pond.":  precio_prom,
            "Importe Bruto Total": total_bruto,
            "Arancel":             arancel,
            "D.Mercado":           d_mercado,
            "Importe Neto":        importe_neto,
        })

    df_ops = pd.DataFrame(operaciones_rows)
    df_res = pd.DataFrame(resumen_rows)

    for df in (df_ops, df_res):
        for col in ("Fecha Concertación", "Fecha Liquidación"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%d/%m/%y", errors="coerce")

    return df_ops, df_res

# --------------------------------------------------
def validate_csv_file(path):
    # 1. Verificar existencia
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"El archivo {path} no existe")

    # 2. Verificar extensión
    if not path.endswith(".csv"):
        raise argparse.ArgumentTypeError(
            f"El archivo {path} no parece ser un archivo CSV"
        )

    # 3. Intentar lectura mínima
    try:
        # Probamos leer la primera fila.
        # Nota: Si tus CSV usan ';' puedes agregar sep=None, engine='python'
        pd.read_csv(
            path,
            nrows=1,
            encoding="ISO-8859-1",
        )
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Error al abrir el archivo CSV {path}: {e}")

    return path


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

    parser = argparse.ArgumentParser(
        description="Migrate from Saldos SSCC in CSV to MongoDB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="csv_path",
        default=os.path.join(path, "saldos_sscc.csv"),
        type=validate_csv_file,
        help="Path al archivo CSV de Saldos SSCC",
    )

    args = parser.parse_args()

    return args


# --------------------------------------------------
class VetaBoletosParser:
    # --------------------------------------------------
    def __init__(self):
        self.clean_df = pd.DataFrame()

        # Repositorios por colección
        # self.rdeu012b2_cuit_repo = Rdeu012b2CuitRepository()

    # --------------------------------------------------
    def from_csv(self, csv_path: str = None) -> pd.DataFrame:
        df = read_csv_file(csv_path)
        df = df.loc[:, ["5", "11", "12", "13", "14"]]
        df.columns = ["ejercicio", "cta_cte", "desc_cta_cte", "desc_banco", "saldo"]
        df["ejercicio"] = df["ejercicio"].str[-4:]
        df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
        df["saldo"] = df["saldo"].str.replace(".", "", regex=False)
        df["saldo"] = df["saldo"].str.replace(",", ".", regex=False)
        df["saldo"] = df["saldo"].astype(float)
        self.clean_df = df
        return self.clean_df

    # --------------------------------------------------
    # async def migrate_deuda_flotante(self):
    #     df = self.from_pdf()
    #     await self.rdeu012b2_cuit_repo.delete_all()
    #     await self.rdeu012b2_cuit_repo.save_all(df.to_dict(orient="records"))

    # --------------------------------------------------
    async def sync_validated_sqlite_to_repository(
        self, sqlite_path: str
    ) -> RouteReturnSchema:
        """Download, process and sync the deuda flotante report to the repository."""
        try:
            df = get_df_from_sql_table(sqlite_path, table="sdo_final_banco_invico")
            df.drop(columns=["id"], inplace=True)
            df["ejercicio"] = pd.to_numeric(df["ejercicio"], errors="coerce")
            df = df.loc[df["ejercicio"] < 2025]

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=BancoINVICOSdoFinalReport,
                field_id="cta_cte",
            )

            return await sync_validated_to_repository(
                repository=BancoINVICOSdoFinalRepository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": {"$lt": 2025}},
                title="Sync SIIF BancoINVICOSdoFinal Report from SQLite",
                logger=logger,
                label="Sync SIIF BancoINVICOSdoFinal Report from SQLite",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")

    # --------------------------------------------------
    async def sync_validated_csv_to_repository(
        self, csv_path: str
    ) -> RouteReturnSchema:
        """Download, process and sync the planillometro report to the repository."""
        try:
            df = self.from_csv(csv_path)

            validate_and_errors = validate_and_extract_data_from_df(
                dataframe=df,
                model=BancoINVICOSdoFinalReport,
                field_id="cta_cte",
            )

            ejercicio = df["ejercicio"].iloc[0] if not df.empty else None

            return await sync_validated_to_repository(
                repository=BancoINVICOSdoFinalRepository(),
                validation=validate_and_errors,
                delete_filter={"ejercicio": int(ejercicio)},
                title="Sync SIIF BancoINVICOSdoFinal Report from CSV",
                logger=logger,
                label="Sync SIIF BancoINVICOSdoFinal Report from CSV",
            )
        except Exception as e:
            print(f"Error migrar y sincronizar el reporte: {e}")


# --------------------------------------------------
async def main():
    # """Make a jazz noise here"""
    # from ...config import Database

    # Database.initialize()
    # try:
    #     await Database.client.admin.command("ping")
    #     print("Connected to MongoDB")
    # except Exception as e:
    #     print("Error connecting to MongoDB:", e)
    #     return

    # args = get_args()
    # try:
    #     migrator = Rdeu012b2Cuit(
    #         pdf_path=args.file,
    #     )

    #     await migrator.migrate_deuda_flotante()
    # except Exception as e:
    #     print(f"Error during migration: {e}")

    """Make a jazz noise here"""

    args = get_args()

    try:
        banco_invico_sdo_final = VetaBoletosParser()
        banco_invico_sdo_final.from_csv(csv_path=args.file)
        print(banco_invico_sdo_final.clean_df)
    except Exception as e:
        print(f"Error al iniciar sesión: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
    # From /invicofapy

    # poetry run python -m src.sscc.handlers.banco_invico_sdo_final
