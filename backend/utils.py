import re
import unicodedata
from datetime import datetime


def converter_float(valor):
    try:
        return float(valor or 0)
    except (TypeError, ValueError):
        return 0.0


def converter_int_nao_negativo(valor):
    texto = str(valor or '').strip()

    if texto and re.fullmatch(r'\d{1,3}([.,]\d{3})+', texto):
        try:
            return max(0, int(texto.replace('.', '').replace(',', '')))
        except (TypeError, ValueError):
            return 0

    try:
        return max(0, int(float(texto or 0)))
    except (TypeError, ValueError):
        return 0


def formatar_moeda_br(valor):
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def formatar_km_br(valor):
    return f"{int(valor):,}".replace(',', '.')


def normalizar_texto(texto):
    base = unicodedata.normalize('NFKD', str(texto or '').lower())
    return ''.join(ch for ch in base if not unicodedata.combining(ch)).strip()


def parse_data_registro(data_texto):
    texto = str(data_texto or '').strip()
    for formato in ('%d/%m/%Y %H:%M', '%d/%m/%Y'):
        try:
            return datetime.strptime(texto, formato)
        except ValueError:
            continue
    return None
