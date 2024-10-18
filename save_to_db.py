import csv
import hashlib
import os
import re
from io import StringIO
from multiprocessing import get_context
from pathlib import Path
from typing import Dict

import psycopg2
from polars import DataFrame, lit, read_csv
from psycopg2.extras import execute_values
from tqdm import tqdm

REGIONS_REGEX = re.compile(
    "(puglia|"
    "liguria|"
    "molise|"
    "toscana|"
    "trentino-alto adige|"
    "campania|"
    "sardegna|"
    "marche|"
    "sicilia|"
    "umbria|"
    "valle d'aosta|"
    "calabria|"
    "veneto|"
    "basilicata|"
    "piemonte|"
    "lazio|"
    "lombardia|"
    "abruzzo|"
    "friuli-venezia giulia|"
    "emilia-romagna)"
)

YEAR_PREFIX_REGEX = re.compile(r"^(\d+-\d+|\d+-i+v? trim.|\d+)")

CONSECUTIVE_WHITESPACE_REGEX = re.compile(" +")

DB_USERNAME = "admin"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = 15432
DB_NAME = "postgres"


def convert_to_dataframe(file: Path) -> DataFrame:
    buffer = StringIO()
    with file.open("r") as f:
        reader = csv.reader(f, delimiter=";", quotechar='"')
        for row in reader:
            if row[-1] == "":
                # Many CSVs have an extra empty column
                row = row[:-1]

            row = [c.strip() for c in row]
            line = '";"'.join(row)

            # Some fields have multiple lines, and tabs too
            line = line.replace("\n", " ").replace("\t", " ")

            # Remove adjacent whitespaces
            line = re.sub(" +", " ", line)

            # Normalize
            line = line.lower()

            buffer.write(f'"{line}"\n')
    buffer.seek(0)
    df = read_csv(
        buffer,
        separator=";",
        infer_schema_length=1_000_000,
        ignore_errors=True,
        truncate_ragged_lines=True,
    )

    # There's typos, column names that are too long and other silly things :(
    renames = {
        "pagamentiin cc": "pagamenti in cc",
        "pagamentiin cr": "pagamenti in cr",
        "valore indicatore missione al netto di tutela della salute anno 1": "val ind miss al netto di tutela della salute anno 1",
        "incidenza missione programma di cui fondo pluriennale vincolato previsione definitiva": "inc miss prog fondo pluriennale vincolato prev definitiva",
        "previsioni iniziali competenza/totale previsioni iniziali competenza": "prev iniziali competenza/totale prev iniziali competenza",
        "valore indicatore missione al netto di tutela della salute anno 2": "val ind miss al netto di tutela della salute anno 2",
        "previsioni definitive competenza/totale previsioni definitive competenza": "prev def competenza/totale prev def competenza",
        "incidenza missione programma di cui fondo pluriennale vincolato dati di rendiconto": "inc miss prog fondo pluriennale vincolato dati di rendiconto",
        "previsioni in cc definitive esercizio precedente di cui utilizzo fondo anticipazione": "prev in cc def es prec utilizzo fondo anticipazione",
        "previsioni in cc definitive esercizio precedente di cui avanzo utilizzato anticipatamente": "prev in cc def es prec avanzo utilizzato anticipatamente",
        "incidenza riscossione prevista nel bilancio di previsione iniziale": "inc riscossione prevista nel bilancio di prev iniziale",
        "incidenza missione programma di cui incidenza economia di spesa dati di rendiconto": "inc miss prog inc economia di spesa dati di rendiconto",
        "incidenza riscossione dei crediti esigibili nell'esercizio finanziario": "inc riscossione dei crediti esigibili nell'es finanziario",
        "incidenza missione programma di cui fondo pluriennale vincolato previsione iniziale": "inc miss prog fondo pluriennale vincolato prev iniziale",
        "capacit di pagamento delle spese esigibili negli esercizi precedenti": "capacit di pagamento delle spese esigibili negli es precedenti",
        "valore indicatore missione al netto di tutela della salute anno 3": "val ind miss al netto di tutela della salute anno 3",
        "incidenza riscossione dei crediti esigibili negli esercizi precedenti": "inc riscossione dei crediti esigibili negli es precedenti",
        "impegni di cui fondo pluriennale vincolato in c/capitale finanziato da debito": "impegni fondo pluriennale vincolato c/cap finanziato da debito",
        "impegni fondo anticipazione di liq. stanziamento definitivo di bilancio": "impegni fondo anticipazione di liq stanziamento def di bilancio",
        "substr(d_cde_pges_piani_gest.text_pges_azio_cod_azione,22,4)": "codice azione",
        "replace(d_cde_pges_piani_gest.desc_pges_azio_desc_azione,chr(10),'')": "azione",
    }
    for old_name, new_name in renames.items():
        if old_name in df.columns:
            df = df.rename({old_name: new_name})
    return df


def save_to_db(file: Path, data: DataFrame):
    connection = (
        f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    table_name = get_table_name(file)

    if data.columns[-1] == "regione":
        # We remove the region from the file name and made it a column.
        # We can differentiate these.
        if_table_exists = "append"
    elif YEAR_PREFIX_REGEX.match(file.stem):
        # We removed the year but that's still present as a column.
        # We can differentiate these too.
        if_table_exists = "append"
    else:
        if_table_exists = "fail"

    try:
        data.write_database(
            table_name=table_name,
            connection=connection,
            if_table_exists=if_table_exists,
        )
    except ValueError:
        # Skip in case the table has already been created
        return


def get_region(file: Path) -> str:
    if match := REGIONS_REGEX.search(file.stem.lower()):
        return match.group(0)
    return ""


def get_table_name(file: Path) -> str:
    table_name = file.stem.lower().strip()

    # Remove region name
    table_name = REGIONS_REGEX.sub("", table_name)
    # Remove year prefix
    table_name = YEAR_PREFIX_REGEX.sub("", table_name)

    # Remove dashes
    table_name = table_name.replace("-", " ")

    # Remove apostrophes
    table_name = table_name.replace("'", "")

    # Remove commas
    table_name = table_name.replace(",", " ")

    # Dots
    table_name = table_name.replace(".", " ")

    # Remove consecutive whitespace
    table_name = CONSECUTIVE_WHITESPACE_REGEX.sub(" ", table_name)

    # Cleanup some more, might be there's some space left after all the replacements
    table_name = table_name.strip()

    # Make all spaces underscores
    table_name = table_name.replace(" ", "_")

    # Most table names would come out toooooo long for postgres.
    # So we just use an hash and store the mapping between the file that
    # created this hash and the original file name.
    # Hope this works lol
    table_name = hashlib.md5(table_name.encode()).hexdigest()
    # Add the data prefix just in case the hash starts with a digit.
    # Can't query tables with name that starts with a digit.
    # This way it's also easier to recognize the tables.
    return f"data_{table_name}"


def create_table_mapping_tables(unique_files: Dict[str, Path]):
    connection = psycopg2.connect(
        user=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
    )

    cursor = connection.cursor()

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS table_data_mapping ("
        "data_name varchar PRIMARY KEY,"
        "table_name varchar"
        ")"
    )
    values = [(k, get_table_name(v)) for k, v in unique_files.items()]
    execute_values(
        cursor,
        "INSERT INTO table_data_mapping (data_name, table_name) VALUES %s "
        "ON CONFLICT (data_name) DO NOTHING",
        values,
    )

    connection.commit()
    cursor.close()
    connection.close()


def process_file(file: Path) -> str:
    with file.open("r") as f:
        if not csv.Sniffer().has_header(f.readline()):
            # Not a valid CSV.
            # Somehow some json error received as response from the API
            # sneaked in and has been saved as CSV.
            # This is a quick check to discrd them.
            return f"Skipped {file.stem}"
    try:
        df = convert_to_dataframe(file)
        if region := get_region(file):
            df = df.with_columns(lit(region).alias("regione"))

        save_to_db(file, df)
    except Exception as exc:
        return f"Error with file {file.stem}: {exc}"
    return f"Saved {file.stem}"


def process_files():
    files = list((Path(__file__).parent / "dataset").glob("**/*.csv"))

    unique_files = {}
    for f in files:
        if f.stem in unique_files:
            continue
        unique_files[f.stem] = f

    create_table_mapping_tables(unique_files)

    # Let's keep a CPU free
    processes = os.cpu_count() - 1
    pool = get_context("spawn").Pool(processes=processes)

    with tqdm(total=len(unique_files.values()), desc="Processing") as progress:
        for res in pool.imap_unordered(
            process_file, unique_files.values(), chunksize=10
        ):
            progress.update()
            tqdm.write(res)

    pool.close()
    pool.join()


if __name__ == "__main__":
    process_files()
