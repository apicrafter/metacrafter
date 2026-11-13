# -*- coding: utf-8 -*-
"""Readers and field names of open data files (issues #96-#103)."""
import types

from metacrafter.core import CrafterCmd


def _scan(path, **kwargs):
    cmd = CrafterCmd(remote=None, debug=False, quiet=True)
    return cmd.scan_file(str(path), limit=50, dformat="full", output_format="json", **kwargs)


def _fields(report):
    rows = report.get("fields") or report["data"]
    return {f["field"].replace("\\.", "."): [m.get("datatype") or m["dataclass"] for m in f["matches"]] for f in rows}


# ---------------------------------------------------------------- #96 _id
def test_id_field_of_jsonl_and_csv_is_reported(tmp_path):
    jsonl = tmp_path / "ids.jsonl"
    jsonl.write_text('{"_id": "6167df39d9af277471403f52", "id": 1, "name": "a"}\n'
                     '{"_id": "5c542a2f34b6ed2f7e45289e", "id": 2, "name": "b"}\n')
    fields = _fields(_scan(jsonl))
    assert list(fields) == ["_id", "id", "name"]
    assert "mongodbid" in fields["_id"]
    csv_path = tmp_path / "ids.csv"
    csv_path.write_text("_id,name\n1,Alpha\n2,Beta\n")
    assert list(_fields(_scan(csv_path))) == ["_id", "name"]


def test_mongodb_scan_still_leaves_collection_ids_out(monkeypatch):
    from metacrafter import sources

    docs = [{"_id": object(), "email": f"user{i}@example.com", "tags": [{"_id": object(), "name": "x"}]}
            for i in range(5)]

    class Cursor(list):
        def batch_size(self, n):
            return self

        def limit(self, n):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __getitem__(self, name):
            return Database()

    class Database:
        def list_collection_names(self):
            return ["users"]

        def __getitem__(self, name):
            return types.SimpleNamespace(find=lambda: Cursor(docs))

    monkeypatch.setattr(sources, "require_extra", lambda *args: types.SimpleNamespace(MongoClient=Client))
    cmd = CrafterCmd(remote=None, debug=False, quiet=True)
    reports, errors = sources.scan_mongo(cmd, "localhost", 27017, "db", None, None, 10, {})
    assert not errors
    assert [f["field"] for f in reports["db.users"]["data"]] == ["email", "tags.name"]


# ---------------------------------------------------------------- #103 field names
def test_field_names_are_stripped(tmp_path):
    path = tmp_path / "spaces.csv"
    path.write_text("stop_id,stop_name       ,wheelchair_boarding   ,stop  desc\n1,A,0,x\n2,B,1,y\n")
    report = _scan(path)
    assert list(_fields(report)) == ["stop_id", "stop_name", "wheelchair_boarding", "stop  desc"]
    entries = {f["field"]: f for f in report["data"]}
    assert entries["wheelchair_boarding"]["name_in_source"] == "wheelchair_boarding   "
    assert "name_in_source" not in entries["stop_id"]


def test_stripping_keeps_names_apart_and_nested_paths():
    from metacrafter.cmd.helpers import strip_key_whitespace

    items = [{"a ": 1, "a": 2, " ": 3, "props": {" NAME ": "x"}, "list": [{"k ": 1}]}]
    renamed = strip_key_whitespace(items)
    assert items == [{"a ": 1, "a": 2, " ": 3, "props": {"NAME": "x"}, "list": [{"k": 1}]}]
    assert renamed == {"props.NAME": "props. NAME ", "list.k": "list.k "}


# ---------------------------------------------------------------- #99 Big5 / CP950
def test_big5_csv_with_cp950_characters(tmp_path):
    from metacrafter.file_inputs import detect_encoding

    # 碁 (0xF9D6) is one of the cp950 characters strict big5 cannot decode.
    text = "販賣業許可證,統一編號,公司名稱,公司地址\n" + "".join(
        f"北市動藥販字{i:04d},0495092{i},丞泰企業有限公司,台北市碁盤路{i}號\n" for i in range(10))
    data = text.encode("cp950")
    assert data.decode(detect_encoding(data, truncated=False)) == text
    path = tmp_path / "sellers.csv"
    path.write_bytes(data)
    assert list(_fields(_scan(path))) == ["販賣業許可證", "統一編號", "公司名稱", "公司地址"]


# ---------------------------------------------------------------- #98 headers below metadata rows
def _sipot_text(columns=12, records=5):
    pad = "\t" * (columns - 1)
    lines = ["59828" + pad,
             "TÍTULO\t\t\tNOMBRE CORTO\t\t\tDESCRIPCIÓN" + "\t" * (columns - 7),
             "Resultados de adjudicación directa\t\t\t28 LGT_Art_70_Fr_XXVIII\t\t\tLa información" + "\t" * (columns - 7),
             "\t".join(str(1 + i % 9) for i in range(columns)),
             "\t".join(str(586933 + i) for i in range(columns)),
             "Tabla Campos" + pad,
             "\t".join(["Ejercicio", "Fecha de inicio del periodo", "Denominación o razón social", "RFC"]
                       + [f"Campo {i}" for i in range(4, columns)])]
    for r in range(records):
        lines.append("\t".join(["2025", "01/07/2025", f"Proveedora {r} S.A. de C.V.", f"ABC0101{r:02d}XY{r}"]
                               + [f"valor {r} {i}" for i in range(4, columns)]))
    return "\n".join(lines) + "\n"


def test_sipot_export_header_after_tabla_campos(tmp_path):
    from metacrafter.file_inputs import table_layout

    text = _sipot_text()
    layout = table_layout(text, "\t", truncated=False)
    assert layout.header_index == 6 and layout.header[:2] == ["Ejercicio", "Fecha de inicio del periodo"]
    path = tmp_path / "sipot.tsv"
    path.write_bytes(text.encode("cp1252"))
    fields = list(_fields(_scan(path)))
    assert fields[:4] == ["Ejercicio", "Fecha de inicio del periodo", "Denominación o razón social", "RFC"]
    assert len(fields) == 12


def _trilingual_rows():
    years = [str(y) for y in range(1990, 2000)]
    blank = [""] * 13
    rows = [
        ["1.05.02.13 Малдын саны", "1.05.02.13 Поголовье скота", "1.05.02.13 Livestock"] + [""] * 10,
        list(blank),
        ["(жылдын аягына карата)", "(на конец года, голов)", "(at the end of the year)"] + [""] * 10,
        ["Баткен облусу", "Баткенская область", "Batken oblast"] + [""] * 10,
        ["Көрсөткүчтөрдүн аталышы ", "Наименование показателей", "Items"] + years,
        list(blank),
        ["Бардык чарбалар", "Хозяйства всех категорий", "Total farms"] + [""] * 10,
    ]
    for name in ("Cattle", "Cows", "Pigs", "Sheep and goats", "Horses"):
        rows.append([name + " (ky)", name + " (ru)", name] + [str(1000 + 7 * i) for i in range(10)])
    return rows


def test_section_headings_above_a_header_of_years():
    from metacrafter.file_inputs import header_row

    index, columns, names = header_row(_trilingual_rows())
    # "Batken oblast" heads the row labels only: a section title, not part of the names.
    assert (index, columns, names) == (4, 13, None)


def test_group_headings_over_value_columns_are_still_joined():
    from metacrafter.file_inputs import header_row

    rows = [["Region", "", "Population", "", "Area", ""],
            ["", "", "2020", "2021", "km2", "mi2"]]
    rows += [[f"R{i}", f"Name {i}", str(10 * i), str(11 * i), str(i), str(i)] for i in range(1, 6)]
    index, _, names = header_row(rows)
    assert index == 1 and names[2:] == ["Population 2020", "Population 2021", "Area km2", "Area mi2"]


def test_title_with_a_unit_note_is_not_a_header_row():
    from metacrafter.file_inputs import header_row

    rows = [[""] * 8, ["（２）年齢（各歳）、男女別人口", "", "", "", "", "", "    単位：人", ""],
            ["年   齢", "総   数", "男", "女", "年   齢", "総   数", "男", "女"], [""] * 8]
    rows += [[f"{a}歳", str(100 + a), str(50 + a), str(50), f"{a + 50}歳", "90", "40", "50"] for a in range(6)]
    assert header_row(rows) == (2, 8, None)


def test_xls_numbers_read_as_the_sheet_shows_them():
    from metacrafter.file_inputs import _xls_cell

    assert [_xls_cell(v) for v in (1990.0, 78.47, "Items", "")] == ["1990", "78.47", "Items", ""]


def test_xlsx_with_trilingual_titles_and_sections(tmp_path):
    openpyxl = __import__("pytest").importorskip("openpyxl")
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    for row in _trilingual_rows():
        sheet.append([int(c) if c.isdigit() else (c or None) for c in row])
    path = tmp_path / "livestock.xlsx"
    workbook.save(path)
    fields = list(_fields(_scan(path)))
    assert fields[:4] == ["Көрсөткүчтөрдүн аталышы", "Наименование показателей", "Items", "1990"]


# ---------------------------------------------------------------- #97 SPSS
FIXTURES = __import__("pathlib").Path(__file__).parent / "fixtures"


def test_spss_file_without_pandas(monkeypatch):
    pytest = __import__("pytest")
    pytest.importorskip("pyreadstat")
    import metacrafter.file_inputs as file_inputs

    # Chunks of 10 rows: the 25 records come from three reads.
    monkeypatch.setattr(file_inputs, "STATS_CHUNK_ROWS", 10)
    plan = file_inputs.prepare_input(str(FIXTURES / "spss_survey.sav"), {})
    records = list(plan.records)
    assert len(records) == 25 and records[0] == {"id": 1.0, "sugu": 2.0, "email": "vastaja0@example.ee", "vanus": None}
    assert plan.field_info["sugu"] == {"label": "Sugu", "value_labels": {"1": "Mees", "2": "Naine"}}

    report = _scan(FIXTURES / "spss_survey.sav")
    fields = _fields(report)
    assert list(fields) == ["id", "sugu", "email", "vanus"] and "email" in fields["email"]
    entries = {f["field"]: f for f in report["data"]}
    assert entries["email"]["label"] == "E-post"
    assert entries["sugu"]["value_labels"] == {"1": "Mees", "2": "Naine"}
    # System-missing values are None, not NaN: the numbers stay a float field.
    assert report["stats"]["vanus"]["ftype"] == "float" and report["stats"]["vanus"]["minval"] == 19.0


def test_compressed_spss_file():
    __import__("pytest").importorskip("pyreadstat")
    assert list(_fields(_scan(FIXTURES / "spss_survey.zsav"))) == ["id", "sugu", "email", "vanus"]


# ---------------------------------------------------------------- #100 KML attributes
KML = """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"><Document><Folder>
<Placemark><name>01</name><ExtendedData><SchemaData schemaUrl="#d"><SimpleData name="COD_DIS">01</SimpleData><SimpleData name="NOMBRE">Centro</SimpleData></SchemaData></ExtendedData><Point><coordinates>-3.70,40.41</coordinates></Point></Placemark>
<Placemark><name>02</name><ExtendedData><Data name="NOMBRE"><value>Arganzuela</value></Data></ExtendedData><Point><coordinates>-3.69,40.40</coordinates></Point></Placemark>
</Folder></Document></kml>
"""

ARCGIS_DESCRIPTION = """<![CDATA[<html><body><table><tr><td>{n}</td></tr><tr><td><table>
<tr><td>FID</td><td>{n}</td></tr><tr bgcolor="#D4E4F3"><td>COD_DIS</td><td>{n:02d}</td></tr>
<tr><td>NOMBRE</td><td>Distrito {n}</td></tr><tr><td>FCH_BAJA</td><td>&lt;Nulo&gt;</td></tr>
</table></td></tr></table></body></html>]]>"""


def test_kml_simpledata_becomes_properties(tmp_path):
    path = tmp_path / "ext.kml"
    path.write_text(KML)
    fields = list(_fields(_scan(path)))
    assert {"properties.COD_DIS", "properties.NOMBRE", "properties.name"} <= set(fields)
    assert fields[0] == "type" and "geometry.coordinates" in fields


def test_kml_arcgis_description_table(tmp_path):
    from metacrafter.file_inputs import prepare_input

    placemarks = "".join(
        f"<Placemark><name>{n}</name><description>{ARCGIS_DESCRIPTION.format(n=n)}</description>"
        f"<MultiGeometry><LineString><coordinates>-3.7,40.4 -3.6,40.5</coordinates></LineString></MultiGeometry>"
        f"</Placemark>" for n in range(1, 4))
    path = tmp_path / "districts.kml"
    path.write_text('<?xml version="1.0" encoding="UTF-8"?><kml xmlns="http://www.opengis.net/kml/2.2">'
                    f"<Document>{placemarks}</Document></kml>")
    records = list(prepare_input(str(path), {}).records)
    assert len(records) == 3
    props = records[0]["properties"]
    assert props["COD_DIS"] == "01" and props["NOMBRE"] == "Distrito 1" and props["FCH_BAJA"] is None
    assert records[0]["geometry"]["type"] == "LineString"


def test_kmz_and_kml_without_namespace(tmp_path):
    import zipfile

    plain = KML.replace(' xmlns="http://www.opengis.net/kml/2.2"', "")
    path = tmp_path / "ext.kmz"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("doc.kml", plain)
    assert {"properties.COD_DIS", "properties.NOMBRE"} <= set(_fields(_scan(path)))


# ---------------------------------------------------------------- #101 XML header and column lists
def _column_xml(encoding="UTF-8", records=4):
    names = ["選挙名", "執行年月日", "当日有権者数計", "投票率（％）計"]
    rows = [["市長選挙", f"平成{9 + r}. 4.27", f"{45693 + r}", f"{78.47 - r:.2f}"] for r in range(records)]
    body = "".join("<body>" + "".join(f"<column>{v}</column>" for v in row) + "</body>" for row in rows)
    header = "<header>" + "".join(f"<column>{n}</column>" for n in names) + "</header>"
    return f'<?xml version="1.0" encoding="{encoding}"?>\n<contents>{header}{body}</contents>\n', names


def test_xml_column_lists_take_the_header_names(tmp_path):
    text, names = _column_xml()
    path = tmp_path / "turnout.xml"
    path.write_text(text, encoding="utf-8")
    assert list(_fields(_scan(path))) == names
    # With the record tag given, too; Shift_JIS files are read in their encoding.
    sjis = tmp_path / "turnout_sjis.xml"
    sjis.write_bytes(_column_xml("Shift_JIS")[0].encode("shift_jis"))
    assert list(_fields(_scan(sjis, tagname="body"))) == names


def test_xml_records_with_different_children_stay_as_they_are(tmp_path):
    from metacrafter.file_inputs import xml_header_names

    data = ("<root><header><column>a</column><column>b</column></header>"
            "<item><name>x</name><email>x@example.com</email></item>"
            "<item><name>y</name><email>y@example.com</email></item></root>").encode()
    assert xml_header_names(data, "item") is None
    # A header with another number of names is not this table's header.
    data = ("<root><header><column>a</column><column>b</column></header>"
            "<body><column>1</column><column>2</column><column>3</column></body></root>").encode()
    assert xml_header_names(data, "body") is None


def test_wide_title_rows_with_unit_notes():
    from metacrafter.file_inputs import header_row

    columns = 20
    title = [""] * columns
    for start in (0, 10):
        title[start], title[start + 6] = "（３）年齢（5歳階級）、男女別人口", " 単位：人"
    header = ["市 町 村"] + [f"{a}～{a + 4}歳" for a in range(0, 5 * (columns - 1), 5)]
    rows = [[""] * columns, title, header]
    rows += [[f"市{i}"] + [str(100 * i + j) for j in range(columns - 1)] for i in range(5)]
    index, _, names = header_row(rows)
    assert index == 2 and names is None


# ---------------------------------------------------------------- #102 workbooks with several tables
def _election_workbook(tmp_path):
    openpyxl = __import__("pytest").importorskip("openpyxl")
    workbook = openpyxl.Workbook()
    notes = workbook.active
    notes.title = "Notas"
    notes.append(["Elecciones municipales 2023"])
    results = workbook.create_sheet("Resultados")
    results.append(["Distrito", "Barrio", "Mesa", "Votos PP", "Votos PSOE"])
    for i in range(6):
        results.append([f"0{i + 1}", f"Barrio {i}", f"{i}-A", 100 + i, 90 + i])
    parties = workbook.create_sheet("Candidaturas")
    parties.append(["Siglas", "Denominación"])
    for short, name in [("PP", "Partido Popular"), ("PSOE", "Partido Socialista Obrero Español"),
                        ("VOX", "Vox"), ("CS", "Ciudadanos-Partido de la Ciudadanía")]:
        parties.append([short, name])
    path = tmp_path / "election.xlsx"
    workbook.save(path)
    return path


def test_default_scan_keeps_one_sheet(tmp_path):
    from metacrafter.file_inputs import prepare_input

    path = _election_workbook(tmp_path)
    assert list(_fields(_scan(path))) == ["Distrito", "Barrio", "Mesa", "Votos PP", "Votos PSOE"]
    assert any("--sheet all" in note for note in prepare_input(str(path), {}).notes)


def test_sheet_all_scans_every_table(tmp_path):
    path = _election_workbook(tmp_path)
    report = _scan(path, sheet="all")
    assert [s["table"] for s in report["sources"]] == ["Resultados", "Candidaturas"]
    assert {s["source"] for s in report["sources"]} == {str(path)}
    by_sheet = {s["table"]: list(_fields(s)) for s in report["sources"]}
    assert by_sheet["Candidaturas"] == ["Siglas", "Denominación"]
    assert by_sheet["Resultados"][:2] == ["Distrito", "Barrio"]
    assert report["complete"] and not report["errors"]


def test_sheet_by_name_or_number(tmp_path, capsys):
    path = _election_workbook(tmp_path)
    assert list(_fields(_scan(path, sheet="Candidaturas"))) == ["Siglas", "Denominación"]
    assert list(_fields(_scan(path, sheet=3))) == ["Siglas", "Denominación"]
    assert _scan(path, sheet="Hoja9") == []
    assert "No sheet 'Hoja9'" in capsys.readouterr().err
    csv_path = tmp_path / "plain.csv"
    csv_path.write_text("a,b\n1,2\n")
    assert _scan(csv_path, sheet="Hoja1") == []
    # Every sheet of a CSV file is the file itself.
    assert list(_fields(_scan(csv_path, sheet="all"))) == ["a", "b"]


def test_ods_sheets(tmp_path):
    pytest = __import__("pytest")
    pytest.importorskip("odf")
    from odf.opendocument import OpenDocumentSpreadsheet
    from odf.table import Table, TableCell, TableRow
    from odf.text import P

    doc = OpenDocumentSpreadsheet()
    for title, rows in (("Leer", []), ("Kreise", [["Kreis", "E-Mail"]] + [[f"Kreis {i}", f"info{i}@kreis.de"]
                                                                          for i in range(4)])):
        table = Table(name=title)
        for values in rows:
            row = TableRow()
            for value in values:
                cell = TableCell(valuetype="string")
                cell.addElement(P(text=value))
                row.addElement(cell)
            table.addElement(row)
        doc.spreadsheet.addElement(table)
    path = tmp_path / "kreise.ods"
    doc.save(str(path))
    assert list(_fields(_scan(path, sheet="Kreise"))) == ["Kreis", "E-Mail"]
    report = _scan(path, sheet="all")
    assert [s["table"] for s in report["sources"]] == ["Kreise"]


def test_cli_sheet_all_writes_an_aggregate(tmp_path):
    import json

    from typer.testing import CliRunner

    import metacrafter.cli.commands  # noqa: F401 - registers the commands
    from metacrafter.core import app

    path = _election_workbook(tmp_path)
    out = tmp_path / "report.json"
    result = CliRunner().invoke(app, ["scan", "file", str(path), "--sheet", "all", "--format", "full",
                                      "--output-format", "json", "-o", str(out), "--quiet"])
    assert result.exit_code == 0, result.output
    payload = json.loads(out.read_text())
    assert [s["table"] for s in payload["sources"]] == ["Resultados", "Candidaturas"]
    assert [f["field"] for f in payload["sources"][1]["fields"]] == ["Siglas", "Denominación"]
    result = CliRunner().invoke(app, ["scan", "file", str(path), "--sheet", "all", "--output-format", "md",
                                      "--stdout", "--quiet"])
    assert result.exit_code == 0 and f"## {path}: Candidaturas" in result.output


# ---------------------------------------------------------------- #105 wide header, short Hebrew header
def _gazetteer_rows(records=8):
    """KSH gazetteer layout: a title, a group heading over the last columns, a header of 20
    columns, and records that leave the last eight columns empty."""
    header = ["Helység megnevezése", "Helység KSH kódja", "Helység jogállása", "Megye megnevezése",
              "Járás kódja", "Járás neve", "Járás székhelye", "Terület (hektár)", "Lakó-népesség",
              "Lakások száma", "Becsült", "Kód"] + ["bolgár", "görög", "horvát", "lengyel", "német",
                                                     "örmény", "roma", "román"]
    rows = [["A helységek részletes adatai 2026. január 1. állapot szerint"] + [""] * 19,
            [""] * 12 + ["Nemzetiségi önkormányzat"] + [""] * 7,
            header]
    for i in range(records):
        rows.append([f"Helység {i}", str(17376 + i), "község", "Fejér", "085 0", "Székesfehérvári",
                     "Székesfehérvár", str(8805 + i), str(4362 + i), str(1571 + i), "", str(i)] + [""] * 8)
    rows[5][19] = "1"  # one record with a minority self-government is as wide as the header
    return rows


def test_header_below_two_title_rows_wider_than_the_records():
    from metacrafter.file_inputs import header_row

    index, columns, names = header_row(_gazetteer_rows())
    assert (index, columns, names) == (2, 20, None)


def test_wide_header_needs_title_rows_above_it():
    from metacrafter.file_inputs import header_row

    rows = _gazetteer_rows()[2:]
    assert header_row(rows)[0] == 0


def test_xlsx_header_below_a_title_and_a_group_heading(tmp_path):
    openpyxl = __import__("pytest").importorskip("openpyxl")
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    for row in _gazetteer_rows():
        sheet.append([c or None for c in row])
    path = tmp_path / "gazetteer.xlsx"
    workbook.save(path)
    fields = list(_fields(_scan(path)))
    assert fields[:5] == ["Helység megnevezése", "Helység KSH kódja", "Helység jogállása", "Megye megnevezése",
                          "Járás kódja"]


def test_short_hebrew_header_in_cp1255(tmp_path):
    from metacrafter.file_inputs import detect_encoding

    text = "OBJECTID\tמספר מקלט\tx\ty\r\n" + "".join(
        f"{200 + i}\t{600 + i}\t220814.8384\t633859.7105\r\n" for i in range(20))
    data = text.encode("cp1255")
    # Detectors guess EUC-KR on these eight bytes ("前初 展麟"): Korean text has Hangul.
    assert detect_encoding(data, truncated=False) == "cp1255"
    path = tmp_path / "shelters.csv"
    path.write_bytes(data)
    assert list(_fields(_scan(path))) == ["OBJECTID", "מספר מקלט", "x", "y"]


def test_korean_and_cyrillic_samples_keep_their_code_pages():
    from metacrafter.file_inputs import _non_latin_choice, detect_encoding

    korean = ("주소,업체명\n" + "".join(f"서울특별시 {i}번지,주식회사 {i}\n" for i in range(5))).encode("cp949")
    assert detect_encoding(korean, truncated=False) in ("cp949", "euc_kr")
    # Russian words break the Hebrew rule of final letters when read as cp1255 (н and к are
    # final forms there), so they decode as Cyrillic.
    assert _non_latin_choice("номер участка".encode("cp1251")) == "cp1251"
    assert _non_latin_choice("שם הרחוב".encode("cp1255")) == "cp1255"
    assert _non_latin_choice("Αριθμός μητρώου".encode("cp1253")) == "cp1253"
    assert _non_latin_choice(b"plain ascii") is None


# ---------------------------------------------------------------- #110 Stata, SAS, dBase, GeoPackage, iCalendar, R
def test_stata_file_without_pandas(tmp_path, monkeypatch):
    pytest = __import__("pytest")
    pyreadstat = pytest.importorskip("pyreadstat")
    pd = pytest.importorskip("pandas")  # only to write the fixture
    import sys

    import metacrafter.file_inputs as file_inputs

    frame = pd.DataFrame({"anio": [2000.0 + i % 3 for i in range(25)],
                          "email": [f"vastaja{i}@example.ee" for i in range(25)],
                          "sugu": [1 + i % 2 for i in range(25)]})
    path = tmp_path / "survey.dta"
    pyreadstat.write_dta(frame, str(path), column_labels={"email": "E-post"},
                         variable_value_labels={"sugu": {1: "Mees", 2: "Naine"}})
    monkeypatch.setitem(sys.modules, "pandas", None)  # iterabledata's reader would need it
    monkeypatch.setattr(file_inputs, "STATS_CHUNK_ROWS", 10)
    plan = file_inputs.prepare_input(str(path), {})
    records = list(plan.records)
    assert plan.kind == "dta" and len(records) == 25
    assert records[0] == {"anio": 2000.0, "email": "vastaja0@example.ee", "sugu": 1}
    assert plan.field_info["sugu"]["value_labels"] == {"1": "Mees", "2": "Naine"}
    report = _scan(path)
    fields = _fields(report)
    assert list(fields) == ["anio", "email", "sugu"] and "email" in fields["email"]
    assert {f["field"]: f for f in report["data"]}["email"]["label"] == "E-post"


def test_sas_files_are_read_with_pyreadstat(tmp_path, monkeypatch):
    pytest = __import__("pytest")
    pyreadstat = pytest.importorskip("pyreadstat")
    pd = pytest.importorskip("pandas")
    import metacrafter.file_inputs as file_inputs

    # SAS transport: pyreadstat writes it, and reads it like a .sas7bdat data set.
    path = tmp_path / "epci.xpt"
    pyreadstat.write_xport(pd.DataFrame({"EPCI": ["200000172", "200000438"], "NB_C": [6.0, 9.0]}), str(path))
    assert list(file_inputs.prepare_input(str(path), {}).records) == [
        {"EPCI": "200000172", "NB_C": 6.0}, {"EPCI": "200000438", "NB_C": 9.0}]
    # A .sas7bdat file goes to pyreadstat.read_sas7bdat with dict output and the given encoding.
    calls = []

    def read_sas7bdat(filename, **kwargs):
        calls.append(kwargs)
        meta = type("Meta", (), {"column_names": ["LIB_EPCI"], "number_columns": 1})()
        return {"LIB_EPCI": ["CC des Quatre Rivières"]}, meta

    monkeypatch.setattr(pyreadstat, "read_sas7bdat", read_sas7bdat)
    sas = tmp_path / "epci.sas7bdat"
    sas.write_bytes(b"")
    plan = file_inputs.prepare_input(str(sas), {"encoding": "cp1252"})
    assert plan.kind == "sas7bdat" and list(plan.records) == [{"LIB_EPCI": "CC des Quatre Rivières"}]
    assert calls[0] == {"row_limit": file_inputs.STATS_CHUNK_ROWS, "output_format": "dict", "encoding": "cp1252"}


def _dbf(fields, rows, encoding, ldid=0):
    """A dBase III table of character fields."""
    import struct

    header_len = 32 + 32 * len(fields) + 1
    record_len = 1 + sum(size for _, size in fields)
    out = bytearray(struct.pack("<BBBBIHH", 3, 126, 1, 1, len(rows), header_len, record_len))
    out += bytes(17) + bytes([ldid]) + bytes(2)
    for name, size in fields:
        out += name.encode("ascii").ljust(11, b"\0") + b"C" + bytes(4) + bytes([size, 0]) + bytes(14)
    out += b"\r"
    for row in rows:
        out += b" " + b"".join(v.encode(encoding).ljust(size, b" ") for v, (_, size) in zip(row, fields))
    return bytes(out + b"\x1a")


DBF_FIELDS = [("NAME", 30), ("CITY", 20)]
DBF_ROWS = [("Оплата праці", "Вінниця"), ("Поточні видатки", "Київ"), ("Нарахування на оплату", "Житомир")]


def test_dbf_code_page_without_language_driver(tmp_path):
    pytest = __import__("pytest")
    pytest.importorskip("dbfread")
    from metacrafter.file_inputs import dbf_encoding, prepare_input

    path = tmp_path / "form2.dbf"
    path.write_bytes(_dbf(DBF_FIELDS, DBF_ROWS, "cp1251"))
    assert dbf_encoding(str(path)) == "cp1251"
    plan = prepare_input(str(path), {})
    assert plan.iterableargs["encoding"] == "cp1251"
    from metacrafter.scan_service import _open_data_file

    reader = _open_data_file(str(path), plan.iterableargs)
    assert [dict(r) for r in reader] == [{"NAME": n, "CITY": c} for n, c in DBF_ROWS]
    assert list(_fields(_scan(path))) == ["NAME", "CITY"]


def test_dbf_language_driver_and_cpg_file(tmp_path):
    from metacrafter.file_inputs import cpg_encoding, dbf_encoding

    __import__("pytest").importorskip("dbfread")
    path = tmp_path / "offices.dbf"
    path.write_bytes(_dbf(DBF_FIELDS, [("Ufficio", "Pisa")], "cp1252", ldid=0x57))
    assert dbf_encoding(str(path)) == "cp1252"
    # A .cpg file next to it wins over the language driver id.
    (tmp_path / "offices.cpg").write_text("ANSI 1251")
    assert dbf_encoding(str(path)) == "cp1251"
    assert [cpg_encoding(t) for t in ("UTF-8", "1251", "65001", "88591", "CP866", "", "nonsense")] == [
        "utf-8", "cp1251", "utf-8", "iso8859-1", "cp866", None, None]


def _wkb_point(x, y, big_endian=False):
    import struct

    order = ">" if big_endian else "<"
    return bytes([0 if big_endian else 1]) + struct.pack(order + "Idd", 1, x, y)


def _gpkg_blob(wkb, envelope=None):
    import struct

    flags = 0x01 | ((1 << 1) if envelope else 0)  # little-endian header, xy envelope
    out = b"GP" + bytes([0, flags]) + struct.pack("<i", 4326)
    if envelope:
        out += struct.pack("<dddd", *envelope)
    return out + wkb


def _geopackage(path):
    import sqlite3

    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE gpkg_spatial_ref_sys (srs_name TEXT, srs_id INTEGER PRIMARY KEY, organization TEXT,
            organization_coordsys_id INTEGER, definition TEXT, description TEXT);
        CREATE TABLE gpkg_contents (table_name TEXT PRIMARY KEY, data_type TEXT, identifier TEXT,
            description TEXT, last_change TEXT, min_x REAL, min_y REAL, max_x REAL, max_y REAL, srs_id INTEGER);
        CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT, geometry_type_name TEXT,
            srs_id INTEGER, z INTEGER, m INTEGER);
        CREATE TABLE "art-objects" (fid INTEGER PRIMARY KEY AUTOINCREMENT, geom BLOB, nosaukums TEXT, type TEXT);
        CREATE TABLE notes (id INTEGER PRIMARY KEY, note TEXT);
        INSERT INTO gpkg_contents (table_name, data_type, identifier) VALUES
            ('notes', 'attributes', 'notes'), ('art-objects', 'features', 'art-objects');
        INSERT INTO gpkg_geometry_columns VALUES ('art-objects', 'geom', 'POINT', 4326, 0, 0);
    """)
    try:
        db.execute('CREATE VIRTUAL TABLE "rtree_art-objects_geom" USING rtree(id, minx, maxx, miny, maxy)')
    except sqlite3.OperationalError:
        pass  # SQLite without the R*Tree module
    db.execute('INSERT INTO "art-objects" (geom, nosaukums, type) VALUES (?, ?, ?)',
               (_gpkg_blob(_wkb_point(24.1, 56.95), envelope=(24.1, 24.1, 56.95, 56.95)), "Brīvības piemineklis", "Piemineklis"))
    db.execute('INSERT INTO "art-objects" (geom, nosaukums, type) VALUES (?, ?, ?)',
               (_gpkg_blob(_wkb_point(24.2, 56.9, big_endian=True)), "Strūklaka", "Strūklaka"))
    db.execute("INSERT INTO notes (note) VALUES ('x')")
    db.commit()
    db.close()


def test_geopackage_feature_table_without_fiona(tmp_path):
    from metacrafter.file_inputs import prepare_input

    path = tmp_path / "art.gpkg"
    _geopackage(str(path))
    for name in ("art.gpkg", "art.sqlite"):  # the same bytes as plain SQLite
        target = tmp_path / name
        if name != "art.gpkg":
            target.write_bytes(path.read_bytes())
        plan = prepare_input(str(target), {})
        records = list(plan.records)
        assert plan.kind == "gpkg" and len(records) == 2
        assert records[0] == {"type": "Feature", "properties": {"nosaukums": "Brīvības piemineklis", "type": "Piemineklis"},
                              "geometry": {"type": "Point", "coordinates": [24.1, 56.95]}}
        assert records[1]["geometry"]["coordinates"] == [24.2, 56.9]
    fields = _fields(_scan(path))
    assert {"properties.nosaukums", "properties.type", "geometry.type", "geometry.coordinates"} <= set(fields)


def test_wkb_lines_polygons_and_collections():
    import struct

    from metacrafter.file_inputs import gpkg_geometry, wkb_geometry

    ring = [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]
    polygon = b"\x01" + struct.pack("<II", 3, 1) + struct.pack("<I", 4) + b"".join(struct.pack("<dd", *p) for p in ring)
    multi = b"\x01" + struct.pack("<II", 6, 2) + polygon + polygon
    assert wkb_geometry(multi)[0] == {"type": "MultiPolygon", "coordinates": [[ring], [ring]]}
    # ISO WKB with z (LineString Z = 1002): x, y and z are kept.
    line_z = b"\x01" + struct.pack("<II", 1002, 2) + struct.pack("<ddd", 1, 2, 3) + struct.pack("<ddd", 4, 5, 6)
    assert wkb_geometry(line_z)[0] == {"type": "LineString", "coordinates": [[1, 2, 3], [4, 5, 6]]}
    collection = b"\x01" + struct.pack("<II", 7, 1) + _wkb_point(1, 2)
    assert gpkg_geometry(_gpkg_blob(collection)) == {
        "type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [1, 2]}]}
    assert gpkg_geometry(b"GP\x00\x11" + bytes(4)) is None  # empty geometry flag
    assert gpkg_geometry("not a blob") is None


def test_sqlite_data_table_comes_before_metadata_tables(tmp_path):
    import sqlite3

    from metacrafter.file_inputs import prepare_input

    path = tmp_path / "roads.sqlite"
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE geometry_columns (f_table_name TEXT, f_geometry_column TEXT);
        CREATE TABLE spatial_ref_sys (srid INTEGER, auth_name TEXT);
        CREATE TABLE roads (id INTEGER, name TEXT, email TEXT);
        INSERT INTO roads VALUES (1, 'Main street', 'roads@example.com');
    """)
    db.commit()
    db.close()
    plan = prepare_input(str(path), {})
    assert plan.records is None and plan.iterableargs["table"] == "roads"
    assert list(_fields(_scan(path))) == ["id", "name", "email"]


ICS = """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Opendatasoft Calendar 1.0//EN
BEGIN:VEVENT
DTSTART:20270505T220000Z
DTEND:20270509T220000Z
UID:29a5c83310ef3025276bdfd7b547d80feff2e005
SUMMARY:Pont de l'Ascension
BEGIN:VALARM
ACTION:DISPLAY
TRIGGER:-PT15M
END:VALARM
END:VEVENT
BEGIN:VEVENT
DTSTART;VALUE=DATE:20271218
UID:d9fcd2780eefabfe09e818d900a59db545ba57bb
SUMMARY:Vacances de Noël
END:VEVENT
END:VCALENDAR
"""


def test_icalendar_events_without_begin_and_end_fields(tmp_path):
    __import__("pytest").importorskip("icalendar")
    from metacrafter.file_inputs import prepare_input

    path = tmp_path / "calendar.ics"
    path.write_text(ICS, encoding="utf-8")
    records = list(prepare_input(str(path), {}).records)
    assert records[0] == {"dtstart": "2027-05-05T22:00:00+00:00", "dtend": "2027-05-09T22:00:00+00:00",
                          "uid": "29a5c83310ef3025276bdfd7b547d80feff2e005", "summary": "Pont de l'Ascension"}
    assert records[1]["dtstart"] == "2027-12-18"
    assert list(_fields(_scan(path))) == ["dtstart", "dtend", "uid", "summary"]


def test_r_data_file(tmp_path):
    pytest = __import__("pytest")
    pyreadr = pytest.importorskip("pyreadr")
    pd = pytest.importorskip("pandas")

    path = tmp_path / "speakers.rda"
    frame = pd.DataFrame({"koneleja": ["KJ1", "KJ2"], "sugu": ["M", "N"], "email": ["a@example.ee", "b@example.ee"]})
    pyreadr.write_rdata(str(path), frame, df_name="fonkorp")
    fields = _fields(_scan(path))
    assert list(fields) == ["koneleja", "sugu", "email"] and "email" in fields["email"]
