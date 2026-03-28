from datetime import datetime


def processar_abastecimento(texto, ultimo_km_registrado):
    partes = texto.split()
    if len(partes) != 3:
        return None

    try:
        valor_total = float(partes[0])
        litros = float(partes[1])
        km_parcial = int(partes[2])
    except (TypeError, ValueError):
        return None

    if ultimo_km_registrado is not None:
        km_antigo_total = int(float(ultimo_km_registrado))
        km_antigo_final = km_antigo_total % 1000
        km_base = (km_antigo_total // 1000) * 1000 + (1000 if km_parcial < km_antigo_final else 0)
        km_final_real = km_base + km_parcial
    else:
        km_final_real = km_parcial

    return {
        'km': km_final_real,
        'litros': litros,
        'preco_total': valor_total,
        'data': datetime.now().strftime('%d/%m/%Y %H:%M')
    }
