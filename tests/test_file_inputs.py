# -*- coding: utf-8 -*-
"""Input preparation for open data files (issues #81-#87)."""
import gzip
import json

import pytest

from metacrafter.core import CrafterCmd
from metacrafter.file_inputs import (
    detect_encoding,
    is_px_metadata_key,
    json_records,
    prepare_input,
    px_encoding,
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


def test_px_encoding_follows_codepage_and_charset():
    assert px_encoding(b'CHARSET="ANSI";\nCODEPAGE="iso-8859-15";\nTITLE="Bev\xf6lkerung";') == "iso8859-15"
    assert px_encoding(b'CHARSET="ANSI";\nTITLE="Bev\xf6lkerung";') == "cp1252"
    assert px_encoding('CHARSET="ANSI";\nTITLE="Bevölkerung";'.encode("utf-8")) == "utf-8"


def test_px_metadata_keys():
    for key in ("_CHARSET", "_AXIS-VERSION", "_SUBJECT-AREA", "_TITLE", "_TITLE[en]", "_NOTE(\"x\")"):
        assert is_px_metadata_key(key)
    for key in ("Gemeinde", "VALUE", "_id", "_", "__x", 5):
        assert not is_px_metadata_key(key)


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
    assert "name" in fields and "email" in fields["email"]


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
