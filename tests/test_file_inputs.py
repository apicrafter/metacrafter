# -*- coding: utf-8 -*-
"""Input preparation for open data files (issues #81-#87)."""
import gzip
import json

import pytest

from metacrafter.core import CrafterCmd
from metacrafter.file_inputs import (
    detect_encoding,
    json_records,
    prepare_input,
    split_name,
    table_layout,
)

GERMAN = "Straße;PLZ;Ort\nMüllerstraße 1;10115;Berlin\nKönigsallee 2;40212;Düsseldorf\n"

LATIN_SAMPLES = [
    ("¿Dónde está la oficina? Señora Muñoz, calle Peñalver 3, España\n", "cp1252"),
    (GERMAN, "cp1252"),
    ("Hôpital Saint-Étienne, rue de la Libération, Besançon; élève\n", "cp1252"),
    ("Città di Forlì, Università, perché più\n", "cp1252"),
    ("São João, Município de Coimbra, Região Centro\n", "cp1252"),
    ("Göteborgs stad, Malmö, Västerås; Åre kommun\n", "cp1252"),
    ("Łódź, ulica Świętokrzyska 12; województwo łódzkie; Gdańsk, Kraków\n", "cp1250"),
    ("Městský úřad Český Krumlov, náměstí Svornosti 1; Plzeň, Brno-Žabovřesky\n", "cp1250"),
    ("Győr, Széchenyi tér; Debrecen, Petőfi utca; Pécs, Dózsa György út\n", "cp1250"),
    ("Vilniaus miesto savivaldybė, Šiaulių gatvė; Klaipėda\n", "cp1257"),
    ("İstanbul şehir merkezi; Ağrı, Çankırı, Şanlıurfa\n", "cp1254"),
]


def _scan(path, **kwargs):
    cmd = CrafterCmd(remote=None, debug=False, quiet=True)
    return cmd.scan_file(str(path), limit=50, dformat="full", output_format="json", **kwargs)


def _fields(report):
    rows = report.get("fields") or report["data"]
    return {f["field"].replace("\\.", "."): [m["datatype"] for m in f["matches"]] for f in rows}


# ---------------------------------------------------------------- encodings (#81, #82)
@pytest.mark.parametrize("text,encoding", LATIN_SAMPLES)
def test_short_latin_samples_get_their_code_page(text, encoding):
    detected = detect_encoding(text.encode(encoding), truncated=False)
    assert text.encode(encoding).decode(detected) == text


@pytest.mark.parametrize(
    "text,encoding",
    [
        ("東京都千代田区;住所;電話番号\n" * 20, "cp932"),
        ("서울특별시 중구;주소\n" * 20, "cp949"),
        ("臺北市中正區;地址\n" * 20, "big5"),
        ("Москва, улица Тверская;адрес\n" * 20, "cp1251"),
        ("مدينة الرياض\n" * 20, "cp1256"),
    ],
)
def test_other_scripts_are_detected(text, encoding):
    assert text.encode(encoding).decode(detect_encoding(text.encode(encoding), truncated=False)) == text


def test_utf8_and_bom():
    assert detect_encoding("Straße".encode("utf-8")) == "utf-8"
    assert detect_encoding("\ufeffid".encode("utf-8")) == "utf-8-sig"
    # A sample cut inside a multi-byte character is still UTF-8.
    assert detect_encoding("Straße".encode("utf-8")[:5], truncated=True) == "utf-8"


# ---------------------------------------------------------------- CSV layout (#85, #87)
def test_split_name():
    assert split_name("/x/a.csv.gz") == ("csv", "gz")
    assert split_name("stops.txt") == ("txt", None)
    assert split_name("counties.shp.zip") == ("shp", "zip")
    assert split_name("README") == ("", None)


def test_sep_hint_sets_delimiter_and_header():
    layout = table_layout("sep=;\nName;PLZ;E-Mail\nA;10115;a@b.de\n", truncated=False)
    assert layout.sep_hint and layout.delimiter == ";" and layout.header_index == 0
    assert layout.header == ["Name", "PLZ", "E-Mail"]


def test_title_rows_above_header_are_found():
    text = "Tabelle 12411-01: Bevölkerung\nStichtag: 31.12.2024\n\nAGS;Gemeinde;Einwohner\n11000000;Berlin;3685265\n"
    layout = table_layout(text, truncated=False)
    assert layout.delimiter == ";" and layout.header_index == 2
    assert layout.header == ["AGS", "Gemeinde", "Einwohner"]


def test_header_with_single_years_of_age_below_a_title():
    # A census table by single years of age (Córdoba DGEyC): the header is mostly the numbers
    # 0 to 100, which count as labels because they run on one by one.
    ages = ";".join(str(a) for a in range(101))
    width = 5 + 101 + 1
    text = (
        "Provincia de Córdoba según radio censal. Personas por edades simples. Año 2010" + ";" * (width - 1) + "\r\n"
        + ";" * (width - 1) + "\r\n"
        + "CODIGO;PROV;DEPTO;FRACCION;RADIO;" + ages + ";Total\r\n"
        + "-;Total Provincial;-;-;-;" + ";".join("52.789" for _ in range(101)) + ";3.308.876\r\n"
        + "140070101;14;7;001;01;" + ";".join(str(a % 4) for a in range(101)) + ";68\r\n"
        + "140070102;14;7;001;02;" + ";".join(str(a % 7) for a in range(101)) + ";494\r\n"
    )
    layout = table_layout(text, truncated=False)
    assert layout.header_index == 2
    assert layout.header[:7] == ["CODIGO", "PROV", "DEPTO", "FRACCION", "RADIO", "0", "1"]
    # Values that only happen to rise by one are no header: four in a row are not enough.
    text = "a;b;c;d;e;f\n1;2;3;4;x;y\n5;6;7;8;z;w\n"
    assert table_layout(text, truncated=False).header_index == 0


def test_ordinary_header_stays_first():
    text = "id;name;city\n1;A;Berlin\n2;B;Hamburg\n"
    assert table_layout(text, truncated=False).header_index == 0
    # A header with one column less than the body is still the header.
    text = "id;name\n1;A;x\n2;B;y\n3;C;z\n"
    assert table_layout(text, truncated=False).header_index == 0


def test_explicit_delimiter_is_kept():
    layout = table_layout("email;name\na@b.c;A\n", delimiter=",", truncated=False)
    assert layout is None or layout.delimiter == ","


# ---------------------------------------------------------------- JSON (#83)
def test_json_records_under_a_key():
    data = json.dumps({"odata.metadata": "x", "value": [{"a": 1}, {"a": 2}]}).encode()
    records, where = json_records(data)
    assert records == [{"a": 1}, {"a": 2}] and "value" in where


def test_json_records_nested_and_plain_object():
    nested = {"success": True, "result": {"total": 2, "records": [{"a": 1}, {"a": 2}]}}
    assert json_records(json.dumps(nested).encode())[0] == [{"a": 1}, {"a": 2}]
    assert json_records(b'{"a": 1, "b": "x"}')[0] == [{"a": 1, "b": "x"}]
    assert json_records(b'[{"a": 1}]') is None


# ---------------------------------------------------------------- scan_file end to end
def test_compressed_cp1252_csv(tmp_path):
    path = tmp_path / "latin1.csv.gz"
    path.write_bytes(gzip.compress(GERMAN.encode("cp1252")))
    assert list(_fields(_scan(path))) == ["Straße", "PLZ", "Ort"]


def test_short_cp1252_csv_is_not_mac_turkish(tmp_path):
    path = tmp_path / "latin1.csv"
    path.write_bytes(GERMAN.encode("cp1252"))
    assert list(_fields(_scan(path))) == ["Straße", "PLZ", "Ort"]


def test_explicit_wrong_encoding_reports_decoding(tmp_path, capsys):
    path = tmp_path / "latin1.csv.gz"
    path.write_bytes(gzip.compress(GERMAN.encode("cp1252")))
    assert _scan(path, encoding="utf-8") == []
    err = capsys.readouterr().err
    assert "could not decode" in err and "--encoding" in err and "Unsupported file type" not in err


def test_pc_axis_ansi(tmp_path):
    px = (
        'CHARSET="ANSI";\nAXIS-VERSION="2000";\nLANGUAGE="de";\nMATRIX="test";\nTITLE="Bevölkerung";\n'
        'CONTENTS="Bevölkerung";\nUNITS="Personen";\nSTUB="Gemeinde";\nHEADING="Jahr";\n'
        'VALUES("Gemeinde")="Zürich","Genève";\nVALUES("Jahr")="2000","2010";\nDATA=\n1 2\n3 4;\n'
    )
    path = tmp_path / "ansi.px"
    path.write_bytes(px.encode("cp1252"))
    assert sorted(_fields(_scan(path))) == ["Gemeinde", "Jahr", "VALUE"]


def test_json_object_root(tmp_path):
    path = tmp_path / "odata.json"
    rows = [{"Gemeente": "Amsterdam", "Email": "info@amsterdam.nl"}, {"Gemeente": "Delft", "Email": "info@delft.nl"}]
    path.write_text(json.dumps({"odata.metadata": "x", "value": rows}))
    fields = _fields(_scan(path))
    assert list(fields) == ["Gemeente", "Email"] and "email" in fields["Email"]


def test_gtfs_stops_txt(tmp_path):
    path = tmp_path / "stops.txt"
    path.write_text("stop_id,stop_name,stop_lat,stop_lon\n1001,Hauptbahnhof,52.5251,13.3694\n"
                    "1002,Alexanderplatz,52.5219,13.4132\n")
    fields = _fields(_scan(path))
    assert list(fields) == ["stop_id", "stop_name", "stop_lat", "stop_lon"]
    assert "latitude" in fields["stop_lat"]


def test_plain_text_txt_stays_text(tmp_path):
    path = tmp_path / "notes.txt"
    path.write_text("Just a sentence.\nAnother one, with a comma.\n")
    assert prepare_input(str(path), {}).iterableargs.get("format") is None


def test_sep_line_and_title_rows(tmp_path):
    sep = tmp_path / "sep.csv"
    sep.write_text("sep=;\nName;PLZ;E-Mail\nSchule A;10115;info@a.de\nSchule B;40212;info@b.de\n")
    fields = _fields(_scan(sep))
    assert list(fields) == ["Name", "PLZ", "E-Mail"] and "email" in fields["E-Mail"]
    title = tmp_path / "title.csv"
    title.write_text("Tabelle 12411-01: Bevölkerung\nStichtag: 31.12.2024\n\n"
                     "AGS;Gemeinde;Einwohner\n11000000;Berlin;3685265\n02000000;Hamburg;1862565\n")
    report = _scan(title)
    assert list(_fields(report)) == ["AGS", "Gemeinde", "Einwohner"]
    assert report["stats"]["Gemeinde"]["n_uniq"] == 2


def test_wfs_gml_members(tmp_path):
    path = tmp_path / "features.gml"
    path.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" '
        'xmlns:gml="http://www.opengis.net/gml/3.2" xmlns:app="http://example.org/app">'
        + "".join(
            f'<wfs:member><app:apotheke gml:id="a{i}"><app:name>Apotheke {i}</app:name>'
            f"<app:email>info{i}@apotheke.de</app:email></app:apotheke></wfs:member>"
            for i in range(5)
        )
        + "</wfs:FeatureCollection>\n"
    )
    fields = _fields(_scan(path))
    # iterabledata reads the members as GeoJSON-like features (properties.<name>).
    email = next(k for k in fields if k.split(".")[-1] == "email")
    assert any(k.split(".")[-1] == "name" for k in fields) and "email" in fields[email]


def test_xlsx_title_rows(tmp_path):
    openpyxl = pytest.importorskip("openpyxl")
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(["Tabelle 1: Kreise nach Fläche und Bevölkerung"])
    sheet.append([])
    sheet.append(["Stand: 31.12.2024"])
    sheet.append(["Schlüssel", "Kreis", "Fläche km²", "Einwohner"])
    for i in range(10):
        sheet.append([f"0{1001 + i}", f"Kreis {i}", 100.5 + i, 1000 * i])
    path = tmp_path / "title.xlsx"
    workbook.save(path)
    assert list(_fields(_scan(path))) == ["Schlüssel", "Kreis", "Fläche km²", "Einwohner"]


def test_zipped_shapefile(tmp_path):
    shapefile = pytest.importorskip("shapefile")
    import zipfile

    base = tmp_path / "points"
    with shapefile.Writer(str(base), shapeType=shapefile.POINT, encoding="cp1252") as writer:
        writer.field("name", "C", 40)
        writer.field("email", "C", 60)
        for i in range(5):
            writer.point(13.4 + i / 100, 52.5)
            writer.record(f"Straße {i}", f"info{i}@example.de")
    (tmp_path / "points.cpg").write_text("cp1252")
    archive = tmp_path / "points.shp.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        for ext in ("shp", "shx", "dbf", "cpg"):
            zf.write(tmp_path / f"points.{ext}", f"points.{ext}")
    fields = _fields(_scan(archive))
    assert "properties.name" in fields and "email" in fields["properties.email"]


# ---------------------------------------------------------------- #89 sheets, multi-row headers
def test_multi_row_header_is_joined():
    text = ("GENESIS-Tabelle: 12411-0018\nBevölkerung: Gemeinden\n"
            ";;Stichtag;Stichtag;Stichtag\n;;31.12.2025;31.12.2025;31.12.2025\n;;Insgesamt;männlich;weiblich\n"
            "01001;Flensburg, Stadt;92550;45614;46936\n01002;Kiel, Landeshauptstadt;248873;121032;127841\n")
    layout = table_layout(text, truncated=False)
    assert layout.header_index == 4
    assert layout.header[2:] == ["Stichtag 31.12.2025 Insgesamt", "Stichtag 31.12.2025 männlich",
                                 "Stichtag 31.12.2025 weiblich"]


def test_text_records_with_empty_cells_are_not_header_rows():
    # AIMS microplastics: the first records are all text with empty cells, fewer filled cells
    # than the records with weights; they were joined into the header ("Polymer cellulose
    # polypropylene nylon").
    rows = ["Sample,tissue,Sample Weight,Shape,colour,Polymer,Contamination Match?,FTIR?,Length"]
    rows += ["Control,,,Fibre,Transparent,cellulose,Match,Yes,"] * 2
    rows += ["Control,,,Fragment,blue,polypropylene,Match,Yes,", "Control,,,Fragment,Transparent,nylon,NoMatch,Yes,"]
    rows += ["Coral Trout 12,Muscle,7.1389,Fibre,Transparent,cellulose,Match,Yes,"] * 3
    layout = table_layout("\n".join(rows) + "\n", truncated=False)
    assert layout.header_index == 0
    assert layout.header[:6] == ["Sample", "tissue", "Sample Weight", "Shape", "colour", "Polymer"]


def test_lower_header_rows_over_value_columns_are_joined():
    text = ("Indicator;2019;2019;2020;2020\n;male;female;male;female\n"
            "Population;10;11;12;13\nHouseholds;4;5;6;7\n")
    layout = table_layout(text, truncated=False)
    assert layout.header_index == 1
    assert layout.header == ["Indicator", "2019 male", "2019 female", "2020 male", "2020 female"]


def test_years_as_column_labels():
    text = "Tabelle 3\n\nIndikator;Einheit;2019;2020;2021\nA;t;1;2;3\nB;t;4;5;6\n"
    layout = table_layout(text, truncated=False)
    assert layout.header_index == 1 and layout.header == ["Indikator", "Einheit", "2019", "2020", "2021"]


def test_text_rows_below_the_header_are_data():
    layout = table_layout("name;city\nAlice;Berlin\nBob;Hamburg\nCarol;Köln\n", truncated=False)
    assert layout.header_index == 0 and layout.header == ["name", "city"]


def test_letterhead_above_a_wide_table():
    columns = 12
    rows = ["KG PSP Warszawa;dn. 22-01-2021" + ";" * (columns - 2), ";" * (columns - 1),
            ";".join(f"col{i}" for i in range(columns))]
    rows += [";".join(str(i * j) for j in range(columns)) for i in range(1, 5)]
    layout = table_layout("\n".join(rows) + "\n", truncated=False)
    assert layout.header_index == 2 and layout.header[0] == "col0"


def test_first_sheet_with_a_table_is_scanned(tmp_path):
    openpyxl = pytest.importorskip("openpyxl")
    workbook = openpyxl.Workbook()
    notes = workbook.active
    notes.title = "Deckblatt"
    notes.append(["Statistisches Bundesamt"])
    notes.append(["Erschienen am 08.11.2026"])
    data = workbook.create_sheet("Kreise")
    data.append(["Kreise nach Fläche und Bevölkerung"])
    data.append(["Schlüssel", "Kreis", "Einwohner", "E-Mail"])
    for i in range(6):
        data.append([f"0{1001 + i}", f"Kreis {i}", 1000 * i, f"info{i}@kreis.de"])
    path = tmp_path / "kreise.xlsx"
    workbook.save(path)
    plan = prepare_input(str(path), {})
    assert any("sheet 2 (Kreise)" in note for note in plan.notes)
    fields = _fields(_scan(path))
    assert list(fields) == ["Schlüssel", "Kreis", "Einwohner", "E-Mail"] and "email" in fields["E-Mail"]


def test_sheet_with_two_header_rows(tmp_path):
    openpyxl = pytest.importorskip("openpyxl")
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(["Estimation de population"])
    sheet.append([])
    sheet.append(["Départements", None, "Ensemble", None, "Hommes", None])
    sheet.append([None, None, "0 à 19 ans", "20 ans et plus", "0 à 19 ans", "20 ans et plus"])
    for i in range(5):
        sheet.append([f"0{i + 1}", f"Dep {i}", 10 * i, 20 * i, 5 * i, 9 * i])
    path = tmp_path / "pop.xlsx"
    workbook.save(path)
    assert list(_fields(_scan(path)))[2:] == ["Ensemble 0 à 19 ans", "Ensemble 20 ans et plus",
                                              "Hommes 0 à 19 ans", "Hommes 20 ans et plus"]


def test_json_column_split_tables():
    pandas_split = {"columns": ["id", "email"], "data": [[1, "a@b.de"], [2, "c@d.de"]]}
    records, where = json_records(json.dumps(pandas_split).encode())
    assert records == [{"id": 1, "email": "a@b.de"}, {"id": 2, "email": "c@d.de"}] and "data" in where
    keyed = {"indices": ["station id", "station latitude"], "data": {"3": ["3", "52.41"], "8": ["8", "51.6"]},
             "count": 2}
    assert json_records(json.dumps(keyed).encode())[0] == [
        {"station id": "3", "station latitude": "52.41"}, {"station id": "8", "station latitude": "51.6"}]


# ---------------------------------------------------------------- formats by content
def test_extensionless_csv_is_read_by_content(tmp_path):
    # data.gov.uk keeps resources as files named csv, xml or json without a dot
    path = tmp_path / "csv"
    path.write_text("SiteReference,Postcode,Email\nBLR0001,LS8 5LN,info@leeds.gov.uk\n"
                    "BLR0002,LS9 7LS,planning@leeds.gov.uk\n")
    plan = prepare_input(str(path), {})
    assert plan.iterableargs["format"] == "csv" and plan.iterableargs["delimiter"] == ","
    fields = _fields(_scan(path))
    assert list(fields) == ["SiteReference", "Postcode", "Email"] and "email" in fields["Email"]


def test_script_extension_csv_is_read_by_content(tmp_path):
    path = tmp_path / "download.aspx"
    path.write_text("Name;PLZ;E-Mail\nSchule A;10115;info@a.de\nSchule B;40212;info@b.de\n")
    assert prepare_input(str(path), {}).iterableargs.get("format") == "csv"
    assert "email" in _fields(_scan(path))["E-Mail"]


def test_extensionless_json_and_json_lines(tmp_path):
    rows = [{"name": "Leeds", "email": "info@leeds.gov.uk"}, {"name": "York", "email": "info@york.gov.uk"}]
    array = tmp_path / "json"
    array.write_text(json.dumps(rows))
    assert prepare_input(str(array), {}).iterableargs["format"] == "json"
    lines = tmp_path / "export"
    lines.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    assert prepare_input(str(lines), {}).iterableargs["format"] == "jsonl"
    assert "email" in _fields(_scan(lines))["email"]


def test_extensionless_xml_and_html(tmp_path):
    xml = tmp_path / "xml"
    xml.write_text('<?xml version="1.0"?><Root><Row><Name>A</Name></Row></Root>')
    assert prepare_input(str(xml), {"tagname": "Row"}).iterableargs["format"] == "xml"
    page = tmp_path / "csv"
    page.write_text("<!DOCTYPE html><html><body>Not found</body></html>")
    assert prepare_input(str(page), {}).iterableargs.get("format") is None


def test_known_extension_is_not_sniffed(tmp_path):
    path = tmp_path / "notes.txt"
    path.write_text("Just a sentence.\nAnother one, with a comma.\n")
    assert prepare_input(str(path), {}).iterableargs.get("format") is None


# ---------------------------------------------------------------- Dataverse .tab exports
def _wide_tab(columns: int) -> str:
    names = ["version", "doi", "email"] + [f"v2x_{i}" for i in range(columns)]
    rows = [
        ['"1.0 (2021-01-01)"', '"doi:10.11587/X8UZ60"', f'"user{n}@example.org"'] + [str(n * i % 7) for i in range(columns)]
        for n in range(1, 6)
    ]
    return "\t".join(names) + "\n" + "\n".join("\t".join(r) for r in rows) + "\n"


def test_tab_file_with_a_header_longer_than_the_sniffed_bytes(tmp_path):
    # V-Dem tables on AUSSDA Dataverse have 3033 variables: the header is 54 kB, and
    # iterabledata, which has no .tab format, could not detect the type from the first bytes.
    path = tmp_path / "10661_da_en_v1_1.tab"
    path.write_text(_wide_tab(3000), encoding="utf-8")
    plan = prepare_input(str(path), {})
    assert plan.iterableargs["format"] == "csv" and plan.iterableargs["delimiter"] == "\t"
    fields = _fields(_scan(path))
    assert len(fields) == 3003 and "email" in fields["email"]


def test_compressed_tab_file(tmp_path):
    path = tmp_path / "export.tab.gz"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write(_wide_tab(5))
    plan = prepare_input(str(path), {})
    assert plan.iterableargs["format"] == "csv" and plan.iterableargs["delimiter"] == "\t"
    assert "email" in _fields(_scan(path))["email"]


# ---------------------------------------------------------------- lines quoted whole
# Exports of datos.gob.ar (Argentina) quote every whole line: the employment by sector series
# of the Ministry of Production write each ;-delimited record as one cell, and the physical
# budget execution tables of the Budget Office write each quoted CSV line as one cell again.
SEMICOLON_LINES = ('"año_mes;provincia;genero;rubro;Cantidad;remuneracion_prom"\r\n'
                   '"39083;Buenos Aires;Femenino;Combustibles-Producción;7;2189,37"\r\n'
                   '"39083;Buenos Aires;Femenino;Metaliferos-Produccion;1;1080,74"\r\n'
                   '"39083;Chubut;Varones;Combustibles-Producción;12;3546,71"\r\n'
                   '"39114;Neuquén;Mujeres;Petróleo y gas;4;2340"\r\n')
NESTED_CSV_LINES = ('"ejercicio_presupuestario,""jurisdiccion_id"",""contacto"",""unidad_medida_desc"""\n'
                    '"2022,""64"",""obras@mop.gob.ar"",""Kilómetro"""\n'
                    '"2022,""64"",""info@mop.gob.ar"",""Obra Terminada"""\n'
                    '"2022,""30"",""presupuesto@mecon.gob.ar"",""Persona Asistida"""\n')


def test_lines_quoted_whole_with_semicolons(tmp_path):
    path = tmp_path / "empleo-rubro-prov.csv"
    path.write_bytes(SEMICOLON_LINES.encode("cp1252"))
    plan = prepare_input(str(path), {})
    assert any("one quoted cell" in note and "';'" in note for note in plan.notes)
    records = list(plan.records)
    assert len(records) == 4 and records[0]["año_mes"] == "39083" and records[3]["provincia"] == "Neuquén"
    fields = _fields(_scan(path))
    assert list(fields) == ["año_mes", "provincia", "genero", "rubro", "Cantidad", "remuneracion_prom"]


def test_quoted_csv_lines_quoted_again(tmp_path):
    path = tmp_path / "inline"
    path.write_text(NESTED_CSV_LINES, encoding="utf-8")
    assert prepare_input(str(path), {}).iterableargs.get("format") == "csv"
    fields = _fields(_scan(path))
    assert list(fields) == ["ejercicio_presupuestario", "jurisdiccion_id", "contacto", "unidad_medida_desc"]
    assert "email" in fields["contacto"]


def test_compressed_lines_quoted_whole(tmp_path):
    path = tmp_path / "empleo.csv.gz"
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        fh.write(SEMICOLON_LINES)
    assert len(list(prepare_input(str(path), {}).records)) == 4


def test_one_column_of_quoted_text_is_not_split(tmp_path):
    path = tmp_path / "direcciones.csv"
    path.write_text('"direccion"\n"Calle 1, Rosario"\n"Avenida 2, Córdoba"\n"Ruta 3, Salta"\n', encoding="utf-8")
    plan = prepare_input(str(path), {})
    assert plan.records is None
    assert list(_fields(_scan(path))) == ["direccion"]


def test_dataverse_column_letters_over_the_header_are_a_title_row():
    # Dataverse names the empty header cells of an ingested sheet after their column: a sheet
    # with a title in A1 is exported as "GRUPOA, B, C, D" above the real header (RDA-UNR).
    text = ("GRUPOA\tB\tC\tD\n"
            '"ID"\t"SEXO"\t"EDAD"\t"TUVO COVID"\n'
            '"0001"\t"Femenino"\t"42"\t"SI"\n"0002"\t"Masculino"\t"48"\t"no"\n"0003"\t"Masculino"\t"47"\t"No"\n')
    layout = table_layout(text, "\t", truncated=False)
    assert layout.header_index == 1
    assert layout.header == ["ID", "SEXO", "EDAD", "TUVO COVID"]


def test_dataverse_column_letters_beside_group_headings_are_blank_cells():
    # Survey of secondary school students (RDA-UNR): question headings over merged cells.
    text = ("A\tB\tPregunta1\tD\tPregunta5\tF\n"
            '"Numero"\t"Edad"\t"Cocaína1"\t"Tabaco1"\t"Cocaína5"\t"Edad cocaína"\n'
            '"1"\t"14"\t"SI"\t"NO"\t"1"\t"13"\n"2"\t"15"\t"NO SE"\t"SI"\t"1"\t"14"\n"3"\t"14"\t"SI"\t"SI"\t"1"\t"12"\n')
    layout = table_layout(text, "\t", truncated=False)
    assert layout.header_index == 1
    assert layout.header == ["Numero", "Edad", "Pregunta1 Cocaína1", "Pregunta1 Tabaco1", "Pregunta5 Cocaína5",
                             "Pregunta5 Edad cocaína"]


def test_dataverse_column_letters_stay_names_of_a_first_row_header():
    # Blank header cells over the second column of each block keep their letter as the name.
    text = ('RT\tB\tMS5.5\tD\tMS6.5\tF\n7.242\t"iC14:0"\t248832.65\t1.95\t86678.76\t0.55\n'
            '7.856\t"nC14:0"\t59423.71\t0.47\t64670.24\t0.41\n8.965\t"iC15:0"\t1464098.52\t11.46\t2514400.95\t15.95\n')
    layout = table_layout(text, "\t", truncated=False)
    assert layout.header_index == 0
    assert layout.header == ["RT", "B", "MS5.5", "D", "MS6.5", "F"]
    letters = "A\tB\tC\nx\ty\tz\n1\t2\t3\n4\t5\t6\n"
    assert table_layout(letters, "\t", truncated=False).header_index == 0


def test_repeated_column_names_are_numbered(tmp_path):
    # Two blocks of one sheet side by side repeat their names; records are dicts, so the
    # second column replaced the first (AquaCrop alfalfa cuts, RDA-UNR; Madrid contracts).
    path = tmp_path / "cuts.tab"
    path.write_text("n\tPeriodo\tkgMS\tF\tn\tPeriodo\tkgMS\n"
                    "1.0\t10-2-00/9-3-00\t2360.0\t\t1.0\t6-11-15 al 17-12-15\t1510.0\n"
                    "2.0\t9-3-00/10-4-00\t1580.0\t\t2.0\t17-12-14 al 16-1-15\t1480.0\n", encoding="utf-8")
    plan = prepare_input(str(path), {})
    assert plan.iterableargs["keys"] == ["n", "Periodo", "kgMS", "F", "n_2", "Periodo_2", "kgMS_2"]
    assert plan.skip_records == 1
    report = _scan(path)
    assert list(_fields(report)) == ["n", "Periodo", "kgMS", "F", "n_2", "Periodo_2", "kgMS_2"]
    assert report["stats"]["Periodo_2"]["n_uniq"] == 2
    # Names that differ in a space are two keys already (Leeds fleet: "Department", "Department ").
    spaced = tmp_path / "fleet.csv"
    spaced.write_text("Department,Department ,Reg\nCity,Parks,AB12CDE\nCity,Roads,CD34EFG\n", encoding="utf-8")
    assert "keys" not in prepare_input(str(spaced), {}).iterableargs


def test_wkt_geometry_longer_than_the_csv_field_limit(tmp_path):
    # GeoServer WFS CSV exports keep the geometry as WKT in one field; a province boundary in
    # marz.csv of ArmSIS has 131365 characters, and the csv module stops at 131072, so the file
    # did not open.
    ring = ", ".join(f"{44 + i / 1e5:.6f} {40 + i / 1e5:.6f}" for i in range(12000))
    path = tmp_path / "marz.csv"
    path.write_text("FID,the_geom,OBJECTID,Name_eng\n"
                    f'marz.1,"MULTIPOLYGON ((({ring})))",1,Yerevan\n'
                    f'marz.2,"MULTIPOLYGON ((({ring})))",2,Aragatsotn\n'
                    f'marz.3,"MULTIPOLYGON ((({ring})))",3,Ararat\n', encoding="utf-8")
    assert len(ring) > 131072
    report = _scan(path)
    assert list(_fields(report)) == ["FID", "the_geom", "OBJECTID", "Name_eng"]
    assert report["stats"]["Name_eng"]["n_uniq"] == 3
    assert report["stats"]["the_geom"]["n_uniq"] == 1
