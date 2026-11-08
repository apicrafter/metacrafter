# Vendored from metacrafter-registry (https://github.com/apicrafter/metacrafter-registry),
# src/metacrafter_registry/checksums.py at commit 4baa8fff0b4387348714505f415dc9b7f855991c,
# Apache-2.0. Metacrafter does not depend on the registry package; record ``checksum`` names
# refer to the keys of ALGORITHMS below. The weekly ecosystem workflow checks that every
# checksum name used by the registry exists here. Do not edit by hand: re-copy instead.
"""Check-digit algorithms referenced by the ``checksum`` field of records.

Each function takes the raw value and returns True when the check digit is
valid. Unless an algorithm says otherwise, separators (spaces, hyphens,
dots) are ignored; any other unexpected character makes the value invalid.
Digits are ASCII digits: other Unicode digits make the value invalid.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from datetime import date

_SEPARATORS = re.compile(r"[\s\-.]")


def _compact(value: str) -> str:
    return _SEPARATORS.sub("", str(value))


def _is_digits(text: str) -> bool:
    """ASCII digits only: str.isdigit() also accepts superscripts, which int() rejects."""
    return text.isascii() and text.isdigit()


def _digits(value: str) -> list[int] | None:
    compact = _compact(value)
    return [int(c) for c in compact] if _is_digits(compact) else None


def luhn(value: str) -> bool:
    """Luhn (mod 10): payment card numbers, IMEI."""
    digits = _digits(value)
    if not digits or len(digits) < 2:
        return False
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def iban_mod97(value: str) -> bool:
    """ISO 13616 IBAN: move the first four characters to the end, mod 97 == 1."""
    compact = _compact(value).upper()
    if not re.fullmatch(r"[A-Z]{2}\d{2}[A-Z0-9]{11,30}", compact):
        return False
    rearranged = compact[4:] + compact[:4]
    return int("".join(str(int(c, 36)) for c in rearranged)) % 97 == 1


def gs1_mod10(value: str) -> bool:
    """GS1 standard check digit of a numeric key of any length.

    Weights 3 and 1 alternate from the right, starting with 3 next to the
    check digit. Used by GLN (13 digits), GSIN (17), GSRN and SSCC (18).
    Leading zeros do not change the result, so an SSCC written with its
    Application Identifier (00) passes too.
    """
    digits = _digits(value)
    if not digits or len(digits) < 2:
        return False
    body, check = digits[:-1], digits[-1]
    total = sum(d * (3 if i % 2 == 0 else 1) for i, d in enumerate(reversed(body)))
    return (10 - total % 10) % 10 == check


def gtin(value: str) -> bool:
    """GS1 mod 10 for GTIN-8, GTIN-12 (UPC), GTIN-13 (EAN) and GTIN-14."""
    digits = _digits(value)
    if not digits or len(digits) not in (8, 12, 13, 14):
        return False
    return gs1_mod10(value)


# GS1 character sets of the General Specifications, in weight order.
_GS1_CSET82 = "!\"%&'()*+,-./0123456789:;<=>?ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"
_GS1_CSET32 = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"


def _primes(count: int) -> list[int]:
    primes: list[int] = []
    candidate = 2
    while len(primes) < count:
        if all(candidate % p for p in primes if p * p <= candidate):
            primes.append(candidate)
        candidate += 1
    return primes


def gs1_checkpair(value: str) -> bool:
    """GS1 check character pair: Global Model Number (GMN) and HIDRI.

    Each character before the pair has its position in CSET 82 as its value.
    The values are weighted by the primes 2, 3, 5, ... from the right and
    summed modulo 1021; the two 5-bit halves of the sum, looked up in CSET 32,
    are the check characters. Hyphens and dots are data here, not separators.
    """
    data = str(value).strip()
    if len(data) < 2:
        return False
    body, pair = data[:-2], data[-2:]
    if any(c not in _GS1_CSET82 for c in body):
        return False
    weights = _primes(len(body))
    total = (
        sum(_GS1_CSET82.index(c) * w for c, w in zip(reversed(body), weights, strict=True)) % 1021
    )
    return pair == _GS1_CSET32[total >> 5] + _GS1_CSET32[total & 31]


def isbn13(value: str) -> bool:
    digits = _digits(value)
    return bool(digits) and len(digits) == 13 and gtin(value)


def isbn10(value: str) -> bool:
    compact = _compact(value).upper()
    if not re.fullmatch(r"\d{9}[\dX]", compact):
        return False
    numbers = [10 if c == "X" else int(c) for c in compact]
    return sum(w * d for w, d in zip(range(10, 0, -1), numbers, strict=True)) % 11 == 0


def issn(value: str) -> bool:
    """ISSN and ISSN-L (ISO 3297): 7 digits and a check digit or X, weights 8 to 1, mod 11."""
    compact = _compact(value).upper()
    if not re.fullmatch(r"[0-9]{7}[0-9X]", compact):
        return False
    numbers = [10 if c == "X" else int(c) for c in compact]
    return sum(w * d for w, d in zip(range(8, 0, -1), numbers, strict=True)) % 11 == 0


def ismn(value: str) -> bool:
    """ISMN (ISO 10957): 13 digits starting with 9790, or the former M and 9 digits.

    The check digit is the GS1 mod 10 check digit of the 13-digit form; the
    letter M of the old form stands for the prefix 979-0.
    """
    compact = _compact(value).upper()
    if re.fullmatch(r"M[0-9]{9}", compact):
        compact = "9790" + compact[1:]
    return bool(re.fullmatch(r"9790[0-9]{9}", compact)) and gtin(compact)


def iso7064_mod11_2(value: str) -> bool:
    """ISO 7064 MOD 11-2: digits and a check character 0-9 or X. Used by ISNI and ORCID."""
    compact = _compact(value).upper()
    if not re.fullmatch(r"[0-9]+[0-9X]", compact):
        return False
    total = 0
    for c in compact[:-1]:
        total = (total + int(c)) * 2 % 11
    check = (12 - total) % 11
    return compact[-1] == ("X" if check == 10 else str(check))


def ru_inn(value: str) -> bool:
    """Russian taxpayer number: 10 digits (organizations) or 12 (individuals)."""
    digits = _digits(value)
    if not digits:
        return False

    def check(weights: list[int]) -> int:
        return sum(w * d for w, d in zip(weights, digits, strict=False)) % 11 % 10

    if len(digits) == 10:
        return check([2, 4, 10, 3, 5, 9, 4, 6, 8]) == digits[9]
    if len(digits) == 12:
        return (
            check([7, 2, 4, 10, 3, 5, 9, 4, 6, 8]) == digits[10]
            and check([3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]) == digits[11]
        )
    return False


def ru_inn_padded(value: str) -> bool:
    """INN of a Russian organization written as 12 digits: 00 and the 10-digit INN.

    Older qualified certificates put it so in the 12-digit INN attribute
    (OID 1.2.643.3.131.1.1)."""
    compact = _compact(value)
    return len(compact) == 12 and compact.startswith("00") and ru_inn(compact[2:])


def ru_snils(value: str) -> bool:
    """Russian insurance number (SNILS): 9 digits and a 2-digit control number."""
    digits = _digits(value)
    if not digits or len(digits) != 11:
        return False
    total = sum(d * (9 - i) for i, d in enumerate(digits[:9]))
    if total > 101:
        total %= 101
    control = 0 if total in (100, 101) else total
    return control == digits[9] * 10 + digits[10]


def ru_ogrn(value: str) -> bool:
    """Russian state registration number: OGRN (13 digits) or OGRNIP (15 digits).

    Besides the check digit (mod 11 for OGRN, mod 13 for OGRNIP), the number
    must start with its sign digit (1 or 5 for OGRN, 3 for OGRNIP), carry a
    registration year (digits 2-3) from the first year the number was issued
    (2002 for OGRN, 2004 for OGRNIP) to the current year, and a region code
    (digits 4-5) other than 00. metacrafter-rules' ``ruogrn`` checks the same.
    """
    compact = _compact(value)
    if not _is_digits(compact):
        return False
    if len(compact) == 13:
        signs, first_year, modulus = "15", 2, 11
    elif len(compact) == 15:
        signs, first_year, modulus = "3", 4, 13
    else:
        return False
    if compact[0] not in signs:
        return False
    if not first_year <= int(compact[1:3]) <= date.today().year % 100:
        return False
    if compact[3:5] == "00":
        return False
    return int(compact[:-1]) % modulus % 10 == int(compact[-1])


def fr_nir(value: str) -> bool:
    """French social security number (NIR): 13 characters and a 2-digit key.

    The key is 97 minus the 13-character number modulo 97; the Corsican
    department codes 2A and 2B count as 19 and 18.
    """
    compact = _compact(value).upper()
    if not re.fullmatch(r"\d{5}(?:\d{2}|2[AB])\d{8}", compact):
        return False
    department = {"2A": "19", "2B": "18"}.get(compact[5:7], compact[5:7])
    body = int(compact[:5] + department + compact[7:13])
    return 97 - body % 97 == int(compact[13:])


def at_uid(value: str) -> bool:
    """Austrian VAT number (UID): U and 8 digits, optionally prefixed with AT.

    The check digit is (6 - Luhn sum of the 7 digits before it) mod 10.
    """
    compact = _compact(value).upper()
    if compact.startswith("AT"):
        compact = compact[2:]
    if not re.fullmatch(r"U\d{8}", compact):
        return False
    digits = [int(c) for c in compact[1:]]
    total = 0
    for i, d in enumerate(reversed(digits[:-1])):
        total += d if i % 2 == 0 else sum(divmod(d * 2, 10))
    return (6 - total) % 10 == digits[-1]


def bg_vat(value: str) -> bool:
    """Bulgarian VAT number: 9 digits (legal entities) or 10 digits, optionally prefixed with BG.

    A 10-digit number passes when its last digit matches the EGN, the PNF or
    the generic check digit; the birth date inside an EGN is not checked.
    """
    compact = _compact(value).upper()
    if compact.startswith("BG"):
        compact = compact[2:]
    digits = [int(c) for c in compact] if _is_digits(compact) else None
    if not digits:
        return False

    def weighted(weights: tuple[int, ...]) -> int:
        return sum(w * d for w, d in zip(weights, digits, strict=False))

    if len(digits) == 9:
        check = weighted((1, 2, 3, 4, 5, 6, 7, 8)) % 11
        if check == 10:
            check = weighted((3, 4, 5, 6, 7, 8, 9, 10)) % 11
        return check % 10 == digits[8]
    if len(digits) == 10:
        egn = weighted((2, 4, 8, 5, 10, 9, 7, 3, 6)) % 11 % 10
        pnf = weighted((21, 19, 17, 13, 11, 9, 7, 3, 1)) % 10
        other = (11 - weighted((4, 3, 2, 7, 6, 5, 4, 3, 2))) % 11
        return digits[9] in (egn, pnf, other)
    return False


def no_fnr(value: str) -> bool:
    """Norwegian national identity number (fødselsnummer, D-number): 11 digits.

    The last two digits are mod-11 check digits; the second one covers the
    first. A check digit that would be 10 makes the number invalid.
    """
    digits = _digits(value)
    if not digits or len(digits) != 11:
        return False

    def check(weights: tuple[int, ...]) -> int:
        return (11 - sum(w * d for w, d in zip(weights, digits, strict=False))) % 11

    return (
        check((3, 7, 6, 1, 8, 9, 4, 5, 2)) == digits[9]
        and check((5, 4, 3, 2, 7, 6, 5, 4, 3, 2)) == digits[10]
    )


_CF_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
# Values of A-Z in odd positions; the digits 0-9 take the values of A-J.
# fmt: off
_CF_ODD_LETTERS = (
    1, 0, 5, 7, 9, 13, 15, 17, 19, 21, 2, 4, 18, 20, 11, 3, 6, 8, 12, 14, 16, 10, 22, 25, 24, 23,
)
# fmt: on
_CF_ODD = {c: _CF_ODD_LETTERS[i if i < 10 else i - 10] for i, c in enumerate(_CF_CHARS)}
_CF_EVEN = {c: i if i < 10 else i - 10 for i, c in enumerate(_CF_CHARS)}


def it_codicefiscale(value: str) -> bool:
    """Italian fiscal code of a person: 16 characters, the last one a check letter.

    Characters in odd positions (1st, 3rd, ...) and even positions have
    separate value tables; the check letter is the sum modulo 26 (A = 0).
    Letters that replace digits on clashes (omocodia) are accepted.
    """
    compact = _compact(value).upper()
    if not re.fullmatch(
        r"[A-Z]{6}[0-9LMNP-V]{2}[ABCDEHLMPRST][0-9LMNP-V]{2}[A-Z][0-9LMNP-V]{3}[A-Z]", compact
    ):
        return False
    total = sum(_CF_ODD[c] if i % 2 == 0 else _CF_EVEN[c] for i, c in enumerate(compact[:15]))
    return chr(ord("A") + total % 26) == compact[15]


def it_iva(value: str) -> bool:
    """Italian VAT number (partita IVA): 11 digits, optionally prefixed with IT.

    Digits 8-10 are the tax office code (001-100, 120, 121, 888 or 999) and
    the last digit is a Luhn check digit.
    """
    compact = _compact(value).upper()
    if compact.startswith("IT"):
        compact = compact[2:]
    if not re.fullmatch(r"\d{11}", compact) or int(compact[:7]) == 0:
        return False
    office = compact[7:10]
    if not ("001" <= office <= "100" or office in ("120", "121", "888", "999")):
        return False
    return luhn(compact)


_AIC_BASE32 = "0123456789BCDFGHJKLMNPQRSTUVWXYZ"


def it_aic(value: str) -> bool:
    """Italian medicine authorisation code (AIC): 9 digits, or 6 base-32 characters.

    The base-32 form (digits and consonants, no A, E, I or O) is converted to
    the 9-digit form, which starts with 0. Digits in even positions (2nd,
    4th, ...) are doubled, the digits of all products are added, and the sum
    modulo 10 is the last digit.
    """
    compact = _compact(value).upper()
    if len(compact) == 6 and all(c in _AIC_BASE32 for c in compact):
        number = sum(_AIC_BASE32.index(c) * 32**i for i, c in enumerate(reversed(compact)))
        compact = str(number).zfill(9)
    if not re.fullmatch(r"0\d{8}", compact):
        return False
    total = sum(
        sum(divmod(int(c) * (1 if i % 2 == 0 else 2), 10)) for i, c in enumerate(compact[:8])
    )
    return total % 10 == int(compact[8])


def fi_hetu(value: str) -> bool:
    """Finnish personal identity code (henkilötunnus): 11 characters, DDMMYYCZZZQ.

    The control character is the 9-digit number DDMMYYZZZ modulo 31 looked up
    in 0123456789ABCDEFHJKLMNPRSTUVWXY. The hyphen is a century sign, so it is
    not stripped; the birth date is not checked.
    """
    compact = re.sub(r"\s", "", str(value)).upper()
    match = re.fullmatch(r"(\d{6})[-+A-FU-Y](\d{3})([0-9A-FHJ-NPR-Y])", compact)
    if not match:
        return False
    return "0123456789ABCDEFHJKLMNPRSTUVWXY"[int(match[1] + match[2]) % 31] == match[3]


def es_dni(value: str) -> bool:
    """Spanish DNI: 8 digits and a check letter, the number modulo 23 in TRWAGMYFPDXBNJZSQVHLCKE."""
    compact = _compact(value).upper()
    if not re.fullmatch(r"\d{8}[A-Z]", compact):
        return False
    return "TRWAGMYFPDXBNJZSQVHLCKE"[int(compact[:8]) % 23] == compact[8]


def _strip_prefix(value: str, prefix: str) -> str:
    compact = _compact(value).upper()
    return compact[len(prefix) :] if compact.startswith(prefix) else compact


def pl_nip(value: str) -> bool:
    """Polish tax number (NIP): 10 digits, optionally prefixed with PL.

    The last digit is the sum of the first nine digits weighted 6 5 7 2 3 4 5
    6 7, modulo 11. A sum that gives 10 makes the number invalid.
    """
    compact = _strip_prefix(value, "PL")
    if not re.fullmatch(r"\d{10}", compact):
        return False
    digits = [int(c) for c in compact]
    weights = (6, 5, 7, 2, 3, 4, 5, 6, 7)
    return sum(w * d for w, d in zip(weights, digits, strict=False)) % 11 == digits[9]


def sk_dph(value: str) -> bool:
    """Slovak VAT number (IČ DPH): 10 digits, optionally prefixed with SK.

    The first digit is not 0 and the whole number is divisible by 11.
    """
    compact = _strip_prefix(value, "SK")
    return bool(re.fullmatch(r"[1-9]\d{9}", compact)) and int(compact) % 11 == 0


def si_ddv(value: str) -> bool:
    """Slovenian VAT number (ID za DDV): 8 digits, optionally prefixed with SI.

    The first digit is not 0. The check digit is 11 minus the sum of the first
    seven digits weighted 8 down to 2, modulo 11; a result of 10 gives 0 and a
    result of 11 makes the number invalid.
    """
    compact = _strip_prefix(value, "SI")
    if not re.fullmatch(r"[1-9]\d{7}", compact):
        return False
    digits = [int(c) for c in compact]
    check = 11 - sum((8 - i) * d for i, d in enumerate(digits[:7])) % 11
    return check != 11 and check % 10 == digits[7]


def se_vat(value: str) -> bool:
    """Swedish VAT number (momsregistreringsnummer): 12 digits, optionally prefixed with SE.

    The first ten digits are the organisation or personal number with a Luhn
    check digit, and the last two are 01.
    """
    compact = _strip_prefix(value, "SE")
    return bool(re.fullmatch(r"\d{10}01", compact)) and luhn(compact[:10])


def kz_iin_bin(value: str) -> bool:
    """Kazakh individual (IIN) or business (BIN) identification number: 12 digits.

    The 12th digit is the sum of the first eleven digits weighted 1 to 11,
    modulo 11. When that gives 10, the weights 3 4 5 6 7 8 9 10 11 1 2 are
    used instead; a second 10 means the number is never issued.
    """
    digits = _digits(value)
    if not digits or len(digits) != 12:
        return False
    check = sum(w * d for w, d in zip(range(1, 12), digits, strict=False)) % 11
    if check == 10:
        weights = (3, 4, 5, 6, 7, 8, 9, 10, 11, 1, 2)
        check = sum(w * d for w, d in zip(weights, digits, strict=False)) % 11
    return check != 10 and check == digits[11]


_BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def base58check(value: str) -> bool:
    """Base58Check with the Bitcoin alphabet.

    The decoded bytes end with a 4-byte checksum: the first four bytes of the
    double SHA-256 of the bytes before it. Each leading '1' stands for a zero
    byte. Used by legacy Bitcoin addresses (P2PKH, P2SH), WIF private keys,
    BIP-32 extended keys and the addresses of Litecoin, Dogecoin and TRON.
    """
    compact = _compact(value)
    if not compact or any(c not in _BASE58 for c in compact):
        return False
    number = 0
    for c in compact:
        number = number * 58 + _BASE58.index(c)
    zeros = len(compact) - len(compact.lstrip("1"))
    raw = b"\0" * zeros + number.to_bytes((number.bit_length() + 7) // 8, "big")
    if len(raw) < 5:
        return False
    payload, checksum = raw[:-4], raw[-4:]
    return hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4] == checksum


_BECH32 = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
_BECH32_GENERATOR = (0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3)


def _bech32_verify(value: str, constant: int) -> bool:
    """BIP-173 checksum: HRP, the separator '1', data characters, 6 of them the checksum.

    Upper or lower case, never mixed. The 90-character limit of BIP-173 is not
    enforced: Lightning invoices and Cardano addresses use longer strings.
    """
    compact = _compact(value)
    if not compact or any(not 33 <= ord(c) <= 126 for c in compact):
        return False
    if compact != compact.lower() and compact != compact.upper():
        return False
    compact = compact.lower()
    pos = compact.rfind("1")
    if pos < 1 or len(compact) - pos - 1 < 6:
        return False
    hrp, data = compact[:pos], compact[pos + 1 :]
    if any(c not in _BECH32 for c in data):
        return False
    values = [ord(c) >> 5 for c in hrp] + [0] + [ord(c) & 31 for c in hrp]
    values += [_BECH32.index(c) for c in data]
    chk = 1
    for v in values:
        top = chk >> 25
        chk = (chk & 0x1FFFFFF) << 5 ^ v
        for i, generator in enumerate(_BECH32_GENERATOR):
            if (top >> i) & 1:
                chk ^= generator
    return chk == constant


def bech32(value: str) -> bool:
    """Bech32 (BIP-173): SegWit v0 addresses, Lightning invoices, Cardano addresses."""
    return _bech32_verify(value, 1)


def bech32m(value: str) -> bool:
    """Bech32m (BIP-350): SegWit v1+ addresses such as Bitcoin Taproot (bc1p...)."""
    return _bech32_verify(value, 0x2BC830A3)


def _iso7064_mod11_10(digits: list[int]) -> bool:
    """ISO 7064 MOD 11,10 over a digit sequence that ends with its check digit."""
    check = 5
    for d in digits:
        check = ((check or 10) * 2 % 11 + d) % 10
    return check == 1


def iso7064_mod97_10(value: str) -> bool:
    """ISO 7064 MOD 97-10 over digits and letters (A=10 ... Z=35): the number mod 97 is 1.

    Used by the German Leitweg-ID, whose last two digits are the check digits.
    """
    compact = _compact(value).upper()
    if not re.fullmatch(r"[0-9A-Z]{3,}", compact):
        return False
    # Fold digit by digit: one big int() fails above 4,300 digits on Python 3.11+.
    remainder = 0
    for c in compact:
        number = int(c, 36)
        remainder = (remainder * (100 if number > 9 else 10) + number) % 97
    return remainder == 1


_ROR_PREFIX = re.compile(r"^(?:https?://)?ror\.org/", re.IGNORECASE)
_CROCKFORD32 = "0123456789abcdefghjkmnpqrstvwxyz"


def ror(value: str) -> bool:
    """ROR identifier: 0, six Crockford base-32 characters and 2 check digits.

    The check digits are ISO 7064 MOD 97-10 of the number n that the six
    characters encode: 98 - (n * 100 mod 97). A https://ror.org/ or ror.org/
    prefix is allowed.
    """
    compact = _ROR_PREFIX.sub("", str(value)).lower()
    match = re.fullmatch(r"0([0-9a-hjkmnp-tv-z]{6})([0-9]{2})", compact)
    if not match:
        return False
    number = 0
    for c in match.group(1):
        number = number * 32 + _CROCKFORD32.index(c)
    return int(match.group(2)) == 98 - number * 100 % 97


def de_ustidnr(value: str) -> bool:
    """German VAT number (USt-IdNr.): 9 digits, optionally prefixed with DE.

    The first digit is not 0 and the last one is an ISO 7064 MOD 11,10 check digit.
    """
    compact = _strip_prefix(value, "DE")
    return bool(re.fullmatch(r"[1-9]\d{8}", compact)) and _iso7064_mod11_10(
        [int(c) for c in compact]
    )


def de_widnr(value: str) -> bool:
    """German business identification number (W-IdNr.): DE, 9 digits and a 5-digit suffix.

    The 9 digits follow the rules of the USt-IdNr.; the suffix (00001, 00002, ...)
    numbers the economic activities and has no check digit.
    """
    compact = _strip_prefix(value, "DE")
    return bool(re.fullmatch(r"\d{14}", compact)) and de_ustidnr(compact[:9])


def de_idnr(value: str) -> bool:
    """German tax identification number (Steuer-IdNr.): 11 digits.

    The first digit is not 0. In the first ten digits exactly one digit occurs
    twice or three times (three equal digits are never all adjacent) and the
    others at most once. The last digit is an ISO 7064 MOD 11,10 check digit.
    """
    digits = _digits(value)
    if not digits or len(digits) != 11 or digits[0] == 0:
        return False
    body = digits[:10]
    repeated = [d for d in set(body) if body.count(d) > 1]
    if len(repeated) != 1 or body.count(repeated[0]) not in (2, 3):
        return False
    if any(body[i] == body[i + 1] == body[i + 2] for i in range(8)):
        return False
    return _iso7064_mod11_10(digits)


def de_rvnr(value: str) -> bool:
    """German pension insurance number (Versicherungsnummer): 12 characters, NN DDMMYY L SS C.

    The letter becomes its two-digit position in the alphabet (A=01 ... Z=26),
    the resulting twelve digits are weighted 2 1 2 5 7 1 2 1 2 1 2 1, the digit
    sums of the products are added and the total modulo 10 is the check digit.
    """
    compact = _compact(value).upper()
    if not re.fullmatch(r"\d{8}[A-Z]\d{3}", compact):
        return False
    number = compact[:8] + f"{ord(compact[8]) - 64:02d}" + compact[9:11]
    weights = (2, 1, 2, 5, 7, 1, 2, 1, 2, 1, 2, 1)
    total = sum(sum(divmod(w * int(c), 10)) for w, c in zip(weights, number, strict=True))
    return total % 10 == int(compact[11])


def de_kvnr(value: str) -> bool:
    """German health insurance number (Krankenversichertennummer): a letter and 9 digits.

    The letter becomes its two-digit position in the alphabet (A=01 ... Z=26).
    These two digits and the next eight are weighted 1 2 1 2 ..., the digit
    sums of the products are added and the total modulo 10 is the last digit.
    """
    compact = _compact(value).upper()
    if not re.fullmatch(r"[A-Z]\d{9}", compact):
        return False
    number = f"{ord(compact[0]) - 64:02d}" + compact[1:9]
    total = sum(sum(divmod((1 + i % 2) * int(c), 10)) for i, c in enumerate(number))
    return total % 10 == int(compact[9])


def de_ik(value: str) -> bool:
    """German institution code (Institutionskennzeichen, IK): 9 digits.

    Digits 3 to 8 are weighted 2 1 2 1 2 1, 9 is subtracted from products above
    9, and the sum modulo 10 is the ninth digit. The first two digits (the
    classification) are not part of the check.
    """
    digits = _digits(value)
    if not digits or len(digits) != 9:
        return False
    total = 0
    for i, d in enumerate(digits[2:8]):
        product = d * (2 if i % 2 == 0 else 1)
        total += product - 9 if product > 9 else product
    return total % 10 == digits[8]


def de_lanr(value: str) -> bool:
    """German lifelong physician number (LANR): 9 digits.

    Digits 1 to 6 are weighted 4 and 9 alternately; the 7th digit is 10 minus
    the sum modulo 10 (10 gives 0). Digits 8 and 9 are the specialty group.
    """
    digits = _digits(value)
    if not digits or len(digits) != 9:
        return False
    total = sum(d * (4 if i % 2 == 0 else 9) for i, d in enumerate(digits[:6]))
    return (10 - total % 10) % 10 == digits[6]


def de_pzn(value: str) -> bool:
    """German pharmacy product number (PZN): 8 digits, or the 7 digits used until 2012.

    An optional PZN prefix is ignored. A 7-digit PZN is read with a leading 0.
    The first seven digits are weighted 1 to 7 and the sum modulo 11 is the
    check digit; a sum that gives 10 is never issued.
    """
    compact = _strip_prefix(value, "PZN")
    if not re.fullmatch(r"\d{7,8}", compact):
        return False
    digits = [int(c) for c in compact.zfill(8)]
    check = sum(w * d for w, d in zip(range(1, 8), digits, strict=False)) % 11
    return check != 10 and check == digits[7]


def de_fsnr(value: str) -> bool:
    """German driving licence number (Führerscheinnummer): 11 letters and digits.

    The first nine characters (authority key and serial) count as base-36
    values (0-9, A=10 ... Z=35), are weighted 9 down to 1, and the sum modulo
    11 is the tenth character, X for 10. The last character numbers the issues.
    """
    compact = _compact(value).upper()
    if not re.fullmatch(r"[0-9A-Z]{9}[0-9X][0-9A-Z]", compact):
        return False
    check = sum(w * int(c, 36) for w, c in zip(range(9, 0, -1), compact, strict=False)) % 11
    return compact[9] == ("X" if check == 10 else str(check))


def cz_ico(value: str) -> bool:
    """Czech identification number of a person or organisation (IČO): 8 digits.

    The first seven digits are weighted 8 down to 2. The check digit is 11
    minus the sum modulo 11, modulo 10: a remainder of 0 gives 1 and a
    remainder of 1 gives 0.
    """
    digits = _digits(value)
    if not digits or len(digits) != 8:
        return False
    total = sum((8 - i) * d for i, d in enumerate(digits[:7]))
    return (11 - total % 11) % 10 == digits[7]


def cz_rc(value: str) -> bool:
    """Czech or Slovak birth number (rodné číslo): YYMMDD and 3 or 4 digits, often after a slash.

    A 10-digit number ends with the remainder of its first nine digits divided
    by 11, where a remainder of 10 gives 0 (used until 1985). 9-digit numbers
    have no check digit and were issued only before 1954. The date is not
    checked.
    """
    compact = _compact(value).replace("/", "")
    if not re.fullmatch(r"\d{9,10}", compact):
        return False
    if len(compact) == 9:
        return not 54 <= int(compact[:2]) <= 79
    return int(compact[:9]) % 11 % 10 == int(compact[9])


def cz_dic(value: str) -> bool:
    """Czech VAT number (DIČ): 8, 9 or 10 digits, optionally prefixed with CZ.

    8 digits (legal entities, the IČO) use the IČO check digit and do not start
    with 9. 9 digits starting with 6 (numbers assigned by the tax authority)
    weight digits 2 to 8 by 8 down to 2; the last digit is 8 minus (10 minus
    the sum modulo 11) modulo 11, modulo 10. Other numbers are birth numbers.
    """
    compact = _strip_prefix(value, "CZ").replace("/", "")
    if not re.fullmatch(r"\d{8,10}", compact):
        return False
    if len(compact) == 8:
        return compact[0] != "9" and cz_ico(compact)
    if len(compact) == 9 and compact[0] == "6":
        total = sum((8 - i) * int(c) for i, c in enumerate(compact[1:8])) % 11
        return (8 - (10 - total) % 11) % 10 == int(compact[8])
    return cz_rc(compact)


def cz_bank_account(value: str) -> bool:
    """Czech bank account number in the national format: [prefix-]number/bank code.

    The prefix has up to 6 digits and the number 2 to 10 digits, at least two
    of them not 0; the bank code has 4 digits. Each part, padded to 10 digits
    and weighted 6 3 7 9 10 5 8 4 2 1, must give a sum divisible by 11
    (Czech National Bank decree 169/2011).
    """
    match = re.fullmatch(r"(?:(\d{1,6})-)?(\d{2,10})/\d{4}", re.sub(r"\s", "", str(value)))
    if not match:
        return False
    prefix, number = match.group(1) or "0", match.group(2)
    if sum(c != "0" for c in number) < 2:
        return False
    weights = (6, 3, 7, 9, 10, 5, 8, 4, 2, 1)
    return all(
        sum(w * int(c) for w, c in zip(weights, part.zfill(10), strict=True)) % 11 == 0
        for part in (prefix, number)
    )


_CZ_ISDS_ALPHABET = "abcdefghijkmnpqrstuvwxyz23456789"


def cz_isds(value: str) -> bool:
    """Czech data box identifier (ID datové schránky): 7 characters, Luhn mod 32.

    The alphabet is abcdefghijkmnpqrstuvwxyz23456789 (no o, l, 0 or 1), giving
    values 0 to 31. Of the first six values those at odd positions (counted
    from 0) are doubled; the quotients and remainders of the results divided
    by 32 are added; the seventh character is 32 minus the sum modulo 32
    (0 stays 0).
    """
    compact = _compact(value).lower()
    if not re.fullmatch(r"[a-km-np-z2-9]{7}", compact):
        return False
    total = 0
    for i, c in enumerate(compact[:6]):
        n = _CZ_ISDS_ALPHABET.index(c) * (2 if i % 2 else 1)
        total += n // 32 + n % 32
    return _CZ_ISDS_ALPHABET[-total % 32] == compact[6]


def be_kbo(value: str) -> bool:
    """Belgian enterprise or establishment unit number (KBO/BCE): 10 digits.

    An optional BE prefix is ignored, and a 9-digit number of the older format
    gets a leading 0. The last two digits are 97 minus the
    remainder of the first eight digits divided by 97.
    """
    compact = _strip_prefix(value, "BE")
    if len(compact) == 9:
        compact = "0" + compact
    if not re.fullmatch(r"\d{10}", compact):
        return False
    return 97 - int(compact[:8]) % 97 == int(compact[8:])


def _be_person_number(value: str, month_ok: Callable[[int], bool]) -> bool:
    compact = _compact(value)
    if not re.fullmatch(r"\d{11}", compact) or not month_ok(int(compact[2:4])):
        return False
    check = int(compact[9:])
    return check in (97 - int(compact[:9]) % 97, 97 - int("2" + compact[:9]) % 97)


def be_nn(value: str) -> bool:
    """Belgian national register number (Rijksregisternummer): 11 digits.

    YYMMDD, a 3-digit serial number and two check digits: 97 minus the
    remainder of the first nine digits divided by 97, where a 2 is put before
    the nine digits for people born from 2000. Both forms of the check are
    accepted whatever the year. The month is 00 to 12 (00 when unknown); the
    date is not otherwise checked.
    """
    return _be_person_number(value, lambda month: month <= 12)


def be_bis(value: str) -> bool:
    """Belgian BIS number: built like the national register number, with 20
    (sex unknown) or 40 (sex known) added to the month: 20 to 32 or 40 to 52."""
    return _be_person_number(value, lambda month: 20 <= month <= 32 or 40 <= month <= 52)


def be_insz(value: str) -> bool:
    """Belgian social security identification number (INSZ/NISS): a national
    register number or a BIS number."""
    return be_nn(value) or be_bis(value)


def be_mod97(value: str) -> bool:
    """Belgian modulo 97 check of a 12-digit number.

    The last two digits are the remainder of the first ten digits divided by
    97, or 97 when the remainder is 0. Used by the eID card number, the
    structured communication (OGM/VCS, as in +++090/9337/55493+++) and the
    national bank account number; slashes, plus signs and asterisks are
    ignored.
    """
    compact = re.sub(r"[/+*]", "", _compact(value))
    if not re.fullmatch(r"\d{12}", compact):
        return False
    return (int(compact[:10]) % 97 or 97) == int(compact[10:])


def _nl_elfproef(digits: list[int]) -> int:
    """Dutch 11-proef sum of 9 digits: weights 9 down to 2, the last digit subtracted, modulo 11."""
    return (sum((9 - i) * d for i, d in enumerate(digits[:8])) - digits[8]) % 11


def nl_bsn(value: str) -> bool:
    """Dutch citizen service number (BSN): 9 digits that pass the 11-proef.

    The first eight digits are weighted 9 down to 2 and the last one -1; the
    sum must be divisible by 11. An 8-digit number is checked with a leading
    zero (RvIG, Logisch Ontwerp BSN). 000000000 is not valid.
    """
    digits = _digits(value)
    if not digits or len(digits) not in (8, 9) or not any(digits):
        return False
    return _nl_elfproef([0] * (9 - len(digits)) + digits) == 0


def nl_onderwijsnummer(value: str) -> bool:
    """Dutch education number (onderwijsnummer): 9 digits starting with 10.

    It uses the weights of the BSN 11-proef, but the sum modulo 11 must be 5.
    """
    digits = _digits(value)
    if not digits or len(digits) != 9 or digits[:2] != [1, 0]:
        return False
    return _nl_elfproef(digits) == 5


def nl_btw(value: str) -> bool:
    """Dutch VAT number (btw-identificatienummer): 9 digits, B and 2 digits, optionally after NL.

    The sequence number after B is not 00. Numbers built from an RSIN or BSN
    pass the 11-proef on their first nine digits; numbers issued to sole
    traders since 2020 instead pass ISO 7064 MOD 97-10 over NL and the whole
    number.
    """
    compact = _strip_prefix(value, "NL")
    if not re.fullmatch(r"\d{9}B\d{2}", compact) or compact[10:] == "00":
        return False
    return nl_bsn(compact[:9]) or iso7064_mod97_10("NL" + compact)


ALGORITHMS: dict[str, Callable[[str], bool]] = {
    "luhn": luhn,
    "iban_mod97": iban_mod97,
    "gtin": gtin,
    "gs1_mod10": gs1_mod10,
    "gs1_checkpair": gs1_checkpair,
    "isbn10": isbn10,
    "isbn13": isbn13,
    "issn": issn,
    "ismn": ismn,
    "iso7064_mod11_2": iso7064_mod11_2,
    "ru_inn": ru_inn,
    "ru_inn_padded": ru_inn_padded,
    "ru_snils": ru_snils,
    "ru_ogrn": ru_ogrn,
    "fr_nir": fr_nir,
    "at_uid": at_uid,
    "bg_vat": bg_vat,
    "no_fnr": no_fnr,
    "it_codicefiscale": it_codicefiscale,
    "it_iva": it_iva,
    "it_aic": it_aic,
    "fi_hetu": fi_hetu,
    "es_dni": es_dni,
    "pl_nip": pl_nip,
    "sk_dph": sk_dph,
    "si_ddv": si_ddv,
    "se_vat": se_vat,
    "kz_iin_bin": kz_iin_bin,
    "base58check": base58check,
    "bech32": bech32,
    "bech32m": bech32m,
    "iso7064_mod97_10": iso7064_mod97_10,
    "ror": ror,
    "de_ustidnr": de_ustidnr,
    "de_widnr": de_widnr,
    "de_idnr": de_idnr,
    "de_rvnr": de_rvnr,
    "de_kvnr": de_kvnr,
    "de_ik": de_ik,
    "de_lanr": de_lanr,
    "de_pzn": de_pzn,
    "de_fsnr": de_fsnr,
    "cz_ico": cz_ico,
    "cz_rc": cz_rc,
    "cz_dic": cz_dic,
    "cz_bank_account": cz_bank_account,
    "cz_isds": cz_isds,
    "be_kbo": be_kbo,
    "be_nn": be_nn,
    "be_bis": be_bis,
    "be_insz": be_insz,
    "be_mod97": be_mod97,
    "nl_bsn": nl_bsn,
    "nl_onderwijsnummer": nl_onderwijsnummer,
    "nl_btw": nl_btw,
}


def check(algorithm: str, value: str) -> bool:
    """Run a named algorithm; raises KeyError for an unknown name."""
    return ALGORITHMS[algorithm](value)
