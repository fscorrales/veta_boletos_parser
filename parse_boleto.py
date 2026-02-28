"""
parse_boleto.py
Lee el HTML de boletos de VETA/ByMA y construye dos DataFrames.
"""

import argparse
import inspect
import os
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup


# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

    parser = argparse.ArgumentParser(
        description="Parsea un archivo HTML de boleto VETA/ByMA y extrae dos DataFrames: uno con las operaciones individuales y otro con el resumen por especie.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-f",
        "--file",
        metavar="html_path",
        default=os.path.join(path, "boleto.htm"),
        type=validate_html_file,
        help="Path al archivo HTML de boleto a parsear (debe ser un archivo .htm o .html válido con tablas) ",
    )

    args = parser.parse_args()

    return args


# --------------------------------------------------
def validate_html_file(path):
    # 1. Verificar existencia
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"El archivo '{path}' no existe")

    # 2. Verificar extensión
    if not path.lower().endswith((".htm", ".html")):
        raise argparse.ArgumentTypeError(
            f"El archivo '{path}' no parece ser un archivo HTML/HTM"
        )

    # 3. Intentar parseo mínimo y verificar que tenga al menos una <table>
    try:
        with open(path, encoding="utf-8-sig") as f:
            soup = BeautifulSoup(f.read(), "html.parser")
        if not soup.find("table"):
            raise argparse.ArgumentTypeError(
                f"El archivo '{path}' no contiene tablas HTML válidas"
            )
    except argparse.ArgumentTypeError:
        raise
    except Exception as e:
        raise argparse.ArgumentTypeError(
            f"Error al abrir el archivo HTML '{path}': {e}"
        )

    return path


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
        print(
            f"⚠  {len(cab_tables)} tablas cabecera / {len(det_tables)} tablas detalle"
        )

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
        fecha_liq = get_cab("Fecha Liquidación")
        operacion = get_cab("Operación").strip()
        especie = get_cab("Especie")

        det_hs = th_names(det_t)
        idx_cant = det_hs.index("Cantidad") if "Cantidad" in det_hs else 0
        idx_precio = det_hs.index("Precio") if "Precio" in det_hs else 1
        idx_bruto = det_hs.index("Importe Bruto") if "Importe Bruto" in det_hs else 2
        idx_detalle = det_hs.index("Detalle") if "Detalle" in det_hs else 8
        idx_gasto = det_hs.index("Gasto") if "Gasto" in det_hs else 10

        arancel = d_mercado = importe_neto = None
        ops_this_block = []

        det_data = [r for r in det_t.find_all("tr") if not r.find("th")]
        for row in det_data:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            cant_raw = cell_text(cells, idx_cant)
            precio_raw = cell_text(cells, idx_precio)
            bruto_raw = cell_text(cells, idx_bruto)
            detalle = cell_text(cells, idx_detalle).upper().strip()
            gasto_raw = cell_text(cells, idx_gasto)

            if detalle == "ARANCEL":
                arancel = to_float(gasto_raw)
            elif detalle == "D.MERCADO":
                d_mercado = to_float(gasto_raw)
            elif detalle == "IMPORTE NETO":
                importe_neto = to_float(gasto_raw)

            cant_val = to_float(cant_raw)
            precio_val = to_float(precio_raw)
            bruto_val = to_float(bruto_raw)

            if (
                cant_val is not None
                and "*" not in cant_raw
                and precio_val is not None
                and bruto_val is not None
            ):
                row_dict = {
                    "Fecha Concertación": fecha_conc,
                    "Fecha Liquidación": fecha_liq,
                    "Operación": operacion,
                    "Especie": especie,
                    "Cantidad": cant_val,
                    "Precio": precio_val,
                    "Importe Bruto": bruto_val,
                }
                operaciones_rows.append(row_dict)
                ops_this_block.append(row_dict)

        total_cant = sum(r["Cantidad"] for r in ops_this_block)
        total_bruto = sum(r["Importe Bruto"] for r in ops_this_block)
        precio_prom = round(total_bruto / total_cant, 6) if total_cant else None

        resumen_rows.append(
            {
                "Fecha Concertación": fecha_conc,
                "Fecha Liquidación": fecha_liq,
                "Operación": operacion,
                "Especie": especie,
                "Cantidad Total": total_cant,
                "Precio Prom. Pond.": precio_prom,
                "Importe Bruto Total": total_bruto,
                "Arancel": arancel,
                "D.Mercado": d_mercado,
                "Importe Neto": importe_neto,
            }
        )

    df_ops = pd.DataFrame(operaciones_rows)
    df_res = pd.DataFrame(resumen_rows)

    for df in (df_ops, df_res):
        for col in ("Fecha Concertación", "Fecha Liquidación"):
            if col in df.columns:
                # df[col] = pd.to_datetime(df[col], format="%d/%m/%y", errors="coerce")
                df[col] = pd.to_datetime(
                    df[col], format="%d/%m/%y", errors="coerce"
                ).dt.date

    return df_ops, df_res


# --------------------------------------------------
def main():
    """Make a jazz noise here"""

    args = get_args()

    try:
        df_ops, df_res = parse_boleto(args.file)
        pd.set_option("display.max_rows", None)
        pd.set_option("display.width", 220)
        pd.set_option("display.float_format", "{:,.4f}".format)

        print("=" * 80)
        print("DataFrame 1 — Operaciones individuales")
        print("=" * 80)
        print(df_ops.to_string(index=False))

        print()
        print("=" * 80)
        print("DataFrame 2 — Resumen por Especie / Bloque")
        print("=" * 80)
        print(df_res.to_string(index=False))

        out_xlsx = Path(args.file).stem + "_parsed.xlsx"
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            df_ops.to_excel(writer, sheet_name="Operaciones", index=False)
            df_res.to_excel(writer, sheet_name="Resumen por Especie", index=False)
        print(f"\n✅  Exportado a: {out_xlsx}")

    except Exception as e:
        print(f"Error al iniciar sesión: {e}")


# --------------------------------------------------
if __name__ == "__main__":
    main()
#     # From /veta_boletos_parser, ejecutar:

#     # poetry run python -m parse_boleto
