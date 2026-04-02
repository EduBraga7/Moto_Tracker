from datetime import datetime, timedelta
from dataclasses import dataclass
from io import BytesIO
import logging
import unicodedata
import requests
import pandas as pd
from fpdf import FPDF
from firebase_admin import firestore

from backend.database import (
    buscar_abastecimentos_por_veiculo,
    buscar_manutencoes_por_veiculo,
    buscar_saude_pecas_por_veiculo,
    buscar_todos_registros,
    sincronizar_km_atual_veiculo,
    upsert_saude_peca,
)
from backend.utils import converter_float, converter_int_nao_negativo, formatar_km_br, formatar_moeda_br, normalizar_texto, parse_data_registro


logger = logging.getLogger(__name__)


def _mapa_limites_configuracao(lista_config):
    limites = {}
    for item in (lista_config or []):
        nome = str(item.get('nome', '')).strip()
        if not nome:
            continue
        limite = converter_int_nao_negativo(item.get('km_vida_util', item.get('km', 0)))
        if limite <= 0:
            continue
        limites[normalizar_texto(nome)] = {
            'nome_peca': nome,
            'km_limite': limite,
        }
    return limites


def _calcular_percentual_peca(km_atual, km_ultima_troca, km_limite):
    km_limite_int = max(1, converter_int_nao_negativo(km_limite))
    km_rodado = max(0, converter_int_nao_negativo(km_atual) - converter_int_nao_negativo(km_ultima_troca))
    percentual_real = (km_rodado / km_limite_int) * 100
    percentual_display = min(100.0, percentual_real)
    km_restante = max(0, km_limite_int - km_rodado)

    if percentual_real <= 70:
        status = 'verde'
    elif percentual_real <= 90:
        status = 'amarelo'
    else:
        status = 'vermelho'

    return {
        'km_rodado': km_rodado,
        'km_restante': km_restante,
        'percentual_real': round(percentual_real, 2),
        'percentual_display': round(percentual_display, 2),
        'status': status,
    }


def _listar_saude_pecas(veiculo, veiculo_id, lista_config=None):
    if not veiculo or not veiculo_id:
        return []

    km_atual = converter_int_nao_negativo(veiculo.get('km_atual', 0))
    limites_config = _mapa_limites_configuracao(lista_config)
    docs_existentes = {}

    for dado in buscar_saude_pecas_por_veiculo(veiculo_id):
        chave = normalizar_texto(dado.get('nome_peca', ''))
        if chave:
            docs_existentes[chave] = dado

    pecas_base = [
        {
            'nome_peca': str(dado.get('nome_peca', 'Peça')).strip() or 'Peça',
            'km_limite': converter_int_nao_negativo(dado.get('km_limite', 0)) or 1000,
        }
        for dado in docs_existentes.values()
    ]
    pecas_base.extend({'nome_peca': item['nome_peca'], 'km_limite': item['km_limite']} for item in limites_config.values())

    saude_pecas = []
    chaves_vistas = set()

    for item in pecas_base:
        nome_peca = item['nome_peca']
        chave = normalizar_texto(nome_peca)
        if chave in chaves_vistas:
            continue
        chaves_vistas.add(chave)

        dado = docs_existentes.get(chave, {})
        limite = converter_int_nao_negativo(dado.get('km_limite', item['km_limite'])) or item['km_limite']
        ultimo = converter_int_nao_negativo(dado.get('ultimo_km_troca', 0))

        if not dado:
            upsert_saude_peca(veiculo_id, nome_peca, ultimo_km_troca=ultimo, km_limite=limite)
        elif limite != converter_int_nao_negativo(dado.get('km_limite', 0)):
            upsert_saude_peca(veiculo_id, nome_peca, km_limite=limite)

        calculo = _calcular_percentual_peca(km_atual, ultimo, limite)
        saude_pecas.append({
            'nome_peca': nome_peca,
            'ultimo_km_troca': ultimo,
            'km_limite': limite,
            'km_rodado': calculo['km_rodado'],
            'km_restante': calculo['km_restante'],
            'porcentagem': calculo['percentual_display'],
            'porcentagem_real': calculo['percentual_real'],
            'status': calculo['status'],
        })

    return saude_pecas


def _coletar_km_maximo_por_veiculo(lista_veiculos):
    veiculos_ids = {str(veiculo.get('id', '')).strip() for veiculo in lista_veiculos if veiculo.get('id')}
    km_por_veiculo = {veiculo_id: 0 for veiculo_id in veiculos_ids}

    if not veiculos_ids:
        return km_por_veiculo

    for dado in buscar_todos_registros('abastecimentos'):
        veiculo_id = str(dado.get('veiculo_id', '')).strip()
        if veiculo_id not in km_por_veiculo:
            continue

        km_registro = converter_int_nao_negativo(dado.get('km', 0))
        if km_registro > km_por_veiculo[veiculo_id]:
            km_por_veiculo[veiculo_id] = km_registro

    return km_por_veiculo


def _coletar_metricas_por_veiculo(lista_veiculos):
    veiculos_ids = {str(veiculo.get('id', '')).strip() for veiculo in lista_veiculos if veiculo.get('id')}
    metricas = {
        veiculo_id: {
            'total_investido': 0.0,
            'custo_ultimos_30_dias': 0.0,
        }
        for veiculo_id in veiculos_ids
    }

    if not veiculos_ids:
        return metricas

    limite_30_dias = datetime.now() - timedelta(days=30)

    for dado in buscar_todos_registros('abastecimentos'):
        veiculo_id = str(dado.get('veiculo_id', '')).strip()
        if veiculo_id not in metricas:
            continue

        valor = converter_float(dado.get('preco_total', dado.get('valor', 0)))
        metricas[veiculo_id]['total_investido'] += valor

        data_registro = parse_data_registro(dado.get('data', ''))
        if data_registro and data_registro >= limite_30_dias:
            metricas[veiculo_id]['custo_ultimos_30_dias'] += valor

    for dado in buscar_todos_registros('manutencoes'):
        veiculo_id = str(dado.get('veiculo_id', '')).strip()
        if veiculo_id not in metricas:
            continue

        valor = converter_float(dado.get('valor', 0))
        metricas[veiculo_id]['total_investido'] += valor

        data_registro = parse_data_registro(dado.get('data', ''))
        if data_registro and data_registro >= limite_30_dias:
            metricas[veiculo_id]['custo_ultimos_30_dias'] += valor

    return metricas


def _enriquecer_veiculos_com_metricas(lista_veiculos):
    for veiculo in lista_veiculos:
        total_investido = converter_float(veiculo.get('total_gastos', 0))
        custo_ultimos_30_dias = converter_float(veiculo.get('custo_ultimos_30_dias', 0))
        km_atual = converter_int_nao_negativo(veiculo.get('km_atual', 0))

        veiculo['total_investido'] = round(total_investido, 2)
        veiculo['total_investido_fmt'] = formatar_moeda_br(total_investido)
        veiculo['custo_medio_mes'] = round(custo_ultimos_30_dias, 2)
        veiculo['custo_medio_mes_fmt'] = formatar_moeda_br(custo_ultimos_30_dias)
        veiculo['km_atual'] = km_atual
        veiculo['km_atual_fmt'] = formatar_km_br(km_atual)


def get_dashboard_data(veiculo_ativo, lista_config=None):
    veiculo_ativo_id = veiculo_ativo.get('id') if veiculo_ativo else None

    lista_abastecimentos = []
    for dado in buscar_abastecimentos_por_veiculo(veiculo_ativo_id):
        dado['km'] = float(dado.get('km', 0))
        dado['litros'] = float(dado.get('litros', 0))
        dado['preco_total'] = float(dado.get('preco_total', dado.get('valor', 0)))
        lista_abastecimentos.append(dado)

    lista_manutencoes = []
    for dado in buscar_manutencoes_por_veiculo(veiculo_ativo_id):
        dado['valor'] = float(dado.get('valor', 0))
        lista_manutencoes.append(dado)

    estatisticas_rodagem = calcular_estatisticas_rodagem(lista_abastecimentos)

    total_gasto_gasolina = sum(a['preco_total'] for a in lista_abastecimentos)
    total_gasto_oficina = sum(m['valor'] for m in lista_manutencoes)
    total_geral = total_gasto_gasolina + total_gasto_oficina

    media_geral = 0
    custo_km = 0
    labels_linha = []
    values_linha = []
    labels_barra = []
    values_barra = []

    if len(lista_abastecimentos) > 1:
        km_inicial = lista_abastecimentos[-1]['km']
        km_final = lista_abastecimentos[0]['km']
        litros_total = sum(a['litros'] for a in lista_abastecimentos[:-1])
        km_rodado_total = km_final - km_inicial

        if litros_total > 0:
            media_geral = km_rodado_total / litros_total
        if km_rodado_total > 0:
            custo_km = total_geral / km_rodado_total

        lista_invertida = lista_abastecimentos[::-1]
        for i in range(len(lista_invertida) - 1):
            atual = lista_invertida[i + 1]
            anterior = lista_invertida[i]
            diff_km = atual['km'] - anterior['km']
            litros = atual['litros']
            if diff_km > 0 and litros > 0:
                kml = diff_km / litros
                data_formatada = atual.get('data', '00/00').split(' ')[0]
                labels_linha.append(data_formatada)
                values_linha.append(round(kml, 1))

    gastos_mensais = {}
    for a in lista_abastecimentos:
        data = a.get('data', '01/01/2000')
        mes_ano = data[3:10]
        gastos_mensais[mes_ano] = gastos_mensais.get(mes_ano, 0) + a['preco_total']

    for m in lista_manutencoes:
        data = m.get('data', '01/01/2000')
        mes_ano = data[3:10]
        gastos_mensais[mes_ano] = gastos_mensais.get(mes_ano, 0) + m['valor']

    for mes in sorted(gastos_mensais.keys()):
        labels_barra.append(mes)
        values_barra.append(round(gastos_mensais[mes], 2))

    km_atual_moto = converter_int_nao_negativo(veiculo_ativo.get('km_atual', 0) if veiculo_ativo else 0)
    km_detectado_abastecimentos = converter_int_nao_negativo(max((a.get('km', 0) for a in lista_abastecimentos), default=0))
    if veiculo_ativo_id and km_detectado_abastecimentos > km_atual_moto:
        km_atual_moto = sincronizar_km_atual_veiculo(veiculo_ativo_id, km_detectado_abastecimentos)
        if veiculo_ativo:
            veiculo_ativo['km_atual'] = km_atual_moto

    saude_pecas = []
    if veiculo_ativo:
        veiculo_ativo['km_atual'] = km_atual_moto
        saude_pecas = _listar_saude_pecas(veiculo_ativo, veiculo_ativo_id, lista_config)
        saude_oleo = next((item for item in saude_pecas if normalizar_texto(item.get('nome_peca', '')) == 'oleo'), None)
        if saude_oleo:
            veiculo_ativo.update({
                'km_rodado': saude_oleo['km_rodado'],
                'km_restante': saude_oleo['km_restante'],
                'km_rodado_oleo': saude_oleo['km_rodado'],
                'km_restante_oleo': saude_oleo['km_restante'],
                'km_limite_oleo': saude_oleo['km_limite'],
                'km_ultima_troca_oleo': saude_oleo['ultimo_km_troca'],
                'km_atual_oleo': km_atual_moto,
                'oleo_percentual': saude_oleo['porcentagem'],
                'status_oleo': saude_oleo['status'],
            })

    abastecimentos_exibicao = []
    for i in range(len(lista_abastecimentos)):
        item = lista_abastecimentos[i].copy()
        item['kml'] = '---'
        item['preco_litro'] = round(item['preco_total'] / item['litros'], 2) if item['litros'] > 0 else 0
        item['data_iso_edit'] = converter_data_padrao_para_iso(item.get('data', ''))
        if i < len(lista_abastecimentos) - 1:
            anterior = lista_abastecimentos[i + 1]
            diff = item['km'] - anterior['km']
            if diff > 0 and item['litros'] > 0:
                calc = diff / item['litros']
                item['kml'] = f'{calc:.1f}'
        abastecimentos_exibicao.append(item)

    return {
        'abastecimentos': abastecimentos_exibicao,
        'manutencoes': lista_manutencoes,
        'saude_pecas': saude_pecas,
        'kpi_media': f'{media_geral:.1f}',
        'kpi_custo_km': f'{custo_km:.2f}',
        'kpi_total': f'{total_geral:.2f}',
        'labels_linha': labels_linha,
        'values_linha': values_linha,
        'labels_barra': labels_barra,
        'values_barra': values_barra,
        'km_total_mes': estatisticas_rodagem['km_total_mes'],
        'media_km_dia': estatisticas_rodagem['media_km_dia'],
        'km_rodado_oleo': (veiculo_ativo or {}).get('km_rodado_oleo', 0),
        'km_restante_oleo': (veiculo_ativo or {}).get('km_restante_oleo', 0),
        'km_limite_oleo': (veiculo_ativo or {}).get('km_limite_oleo', 1000),
        'km_ultima_troca_oleo': (veiculo_ativo or {}).get('km_ultima_troca_oleo', 0),
        'km_atual_oleo': (veiculo_ativo or {}).get('km_atual_oleo', km_atual_moto),
        'veiculo_ativo': veiculo_ativo,
    }


@dataclass
class Veiculo:
    id: str
    apelido: str
    fipe_codigo: str
    ano_modelo: int
    valor_fipe: str
    km_atual: int
    ultimo_oleo_km: int = 0


@dataclass
class Abastecimento:
    km: float
    litros: float
    preco_total: float
    data: str
    veiculo_id: str


@dataclass
class Manutencao:
    km: float
    servico: str
    valor: float
    data: str
    obs: str
    veiculo_id: str


def processar_abastecimento(texto, ultimo_km_registrado, veiculo_id=None):
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

    media_kml = None
    if ultimo_km_registrado is not None and litros > 0:
        km_rodado = km_final_real - int(float(ultimo_km_registrado))
        if km_rodado > 0:
            media_kml = km_rodado / litros

    abastecimento = Abastecimento(
        km=km_final_real,
        litros=litros,
        preco_total=valor_total,
        data=datetime.now().strftime('%d/%m/%Y %H:%M'),
        veiculo_id=str(veiculo_id or ''),
    )

    return {
        'km': abastecimento.km,
        'litros': abastecimento.litros,
        'preco_total': abastecimento.preco_total,
        'data': abastecimento.data,
        'media_kml': media_kml,
        'veiculo_id': abastecimento.veiculo_id,
    }


def montar_payload_veiculo(id_veiculo, apelido, fipe_codigo, ano_modelo, valor_fipe, marca='', modelo='', mes_referencia='', km_atual=0, ultimo_oleo_km=0):
    veiculo = Veiculo(
        id=str(id_veiculo or '').strip(),
        apelido=str(apelido or '').strip(),
        fipe_codigo=str(fipe_codigo or '').strip(),
        ano_modelo=int(ano_modelo or 0),
        valor_fipe=str(valor_fipe or '').strip(),
        km_atual=int(float(km_atual or 0)),
        ultimo_oleo_km=int(float(ultimo_oleo_km or 0)),
    )

    return {
        'id': veiculo.id,
        'apelido': veiculo.apelido,
        'fipe_codigo': veiculo.fipe_codigo,
        'ano_modelo': veiculo.ano_modelo,
        'valor_fipe': veiculo.valor_fipe,
        'km_atual': veiculo.km_atual,
        'ultimo_oleo_km': veiculo.ultimo_oleo_km,
        'marca': str(marca or '').strip(),
        'modelo': str(modelo or '').strip(),
        'mes_referencia': str(mes_referencia or '').strip(),
    }


def montar_payload_abastecimento(km, litros, preco_total, data_registro, veiculo_id):
    abastecimento = Abastecimento(
        km=float(km),
        litros=float(litros),
        preco_total=float(preco_total),
        data=str(data_registro or '').strip(),
        veiculo_id=str(veiculo_id or '').strip(),
    )

    return {
        'km': abastecimento.km,
        'litros': abastecimento.litros,
        'preco_total': abastecimento.preco_total,
        'data': abastecimento.data,
        'veiculo_id': abastecimento.veiculo_id,
    }


def montar_payload_manutencao(km, servico, valor, data_padrao, obs='', veiculo_id=None):
    manutencao = Manutencao(
        km=float(km),
        servico=str(servico or '').strip(),
        valor=float(valor),
        data=str(data_padrao or '').strip(),
        obs=str(obs or '').strip(),
        veiculo_id=str(veiculo_id or '').strip(),
    )

    return {
        'km': manutencao.km,
        'servico': manutencao.servico,
        'valor': manutencao.valor,
        'obs': manutencao.obs,
        'data': manutencao.data,
        'veiculo_id': manutencao.veiculo_id,
    }


def calcular_estatisticas_rodagem(lista_abastecimentos):
    hoje = datetime.now()
    mes_atual = hoje.month
    ano_atual = hoje.year

    if mes_atual == 1:
        mes_passado = 12
        ano_mes_passado = ano_atual - 1
    else:
        mes_passado = mes_atual - 1
        ano_mes_passado = ano_atual

    registros_mes_atual = []
    registros_mes_passado = []

    for item in lista_abastecimentos:
        data_texto = str(item.get('data', '')).strip()
        try:
            data_registro = datetime.strptime(data_texto, '%d/%m/%Y %H:%M')
        except ValueError:
            try:
                data_registro = datetime.strptime(data_texto, '%d/%m/%Y')
            except ValueError:
                continue

        km = float(item.get('km', 0) or 0)

        if data_registro.month == mes_atual and data_registro.year == ano_atual:
            registros_mes_atual.append((data_registro, km))
        elif data_registro.month == mes_passado and data_registro.year == ano_mes_passado:
            registros_mes_passado.append((data_registro, km))

    if not registros_mes_atual:
        return {'km_total_mes': '0', 'media_km_dia': '0.0'}

    registros_mes_atual.sort(key=lambda item: item[0])
    primeiro_km_mes_atual = registros_mes_atual[0][1]
    ultimo_km_mes_atual = registros_mes_atual[-1][1]

    if registros_mes_passado:
        registros_mes_passado.sort(key=lambda item: item[0])
        km_base = registros_mes_passado[-1][1]
    else:
        km_base = primeiro_km_mes_atual

    km_total_mes = max(0.0, ultimo_km_mes_atual - km_base)
    media_km_dia = km_total_mes / max(hoje.day, 1)

    return {
        'km_total_mes': f'{km_total_mes:.0f}',
        'media_km_dia': f'{media_km_dia:.1f}'
    }


def _normalizar_texto(valor):
    texto = str(valor or '').strip().lower()
    texto = unicodedata.normalize('NFKD', texto)
    return ''.join(ch for ch in texto if not unicodedata.combining(ch))


def calcular_pecas_monitoradas(lista_config, lista_manutencoes, km_atual_moto):
    pecas_monitoradas = []
    km_atual = float(km_atual_moto or 0)

    for config in lista_config:
        nome_peca = str(config.get('nome', 'Peça')).strip() or 'Peça'

        try:
            km_vida_util = float(config.get('km_vida_util', config.get('km', 1000)) or 1000)
        except (TypeError, ValueError):
            km_vida_util = 1000.0

        if km_vida_util <= 0:
            km_vida_util = 1000.0

        nome_busca = _normalizar_texto(nome_peca)
        km_ultima_troca = 0.0

        for manutencao in lista_manutencoes:
            servico = _normalizar_texto(manutencao.get('servico', ''))
            if nome_busca and nome_busca in servico:
                try:
                    km_ultima_troca = float(manutencao.get('km', 0) or 0)
                except (TypeError, ValueError):
                    km_ultima_troca = 0.0
                break

        km_rodado = max(0.0, km_atual - km_ultima_troca)
        km_restante = km_vida_util - km_rodado
        percentual = (km_restante / km_vida_util) * 100
        percentual_limitado = max(0.0, min(100.0, percentual))

        if percentual_limitado >= 50:
            vida_class = 'vida-ok'
        elif percentual_limitado >= 20:
            vida_class = 'vida-alerta'
        else:
            vida_class = 'vida-critica'

        pecas_monitoradas.append({
            'nome': nome_peca,
            'porcentagem': percentual_limitado,
            'vida_class': vida_class,
            'km_restante': km_restante,
            'km_rodado': km_rodado
        })

    return pecas_monitoradas


def converter_data_padrao_para_iso(data_texto):
    texto = str(data_texto or '').strip()
    formatos_origem = ('%d/%m/%Y %H:%M', '%d/%m/%Y')

    for formato in formatos_origem:
        try:
            data_obj = datetime.strptime(texto, formato)
            return data_obj.strftime('%Y-%m-%dT%H:%M')
        except ValueError:
            continue

    return datetime.now().strftime('%Y-%m-%dT%H:%M')


def converter_data_iso_para_padrao(data_iso):
    texto = str(data_iso or '').strip()

    if not texto:
        return datetime.now().strftime('%d/%m/%Y %H:%M')

    try:
        data_obj = datetime.strptime(texto, '%Y-%m-%dT%H:%M')
    except ValueError:
        return datetime.now().strftime('%d/%m/%Y %H:%M')

    return data_obj.strftime('%d/%m/%Y %H:%M')


def calcular_desgaste_pecas(km_atual, lista_manutencoes=None):
    limites_padrao = {
        'Óleo': 1000,
        'Relação': 15000,
        'Pneus': 12000,
    }

    manutencoes = lista_manutencoes or []

    try:
        km_atual_float = float(km_atual or 0)
    except (TypeError, ValueError):
        km_atual_float = 0.0

    pecas_dinamicas = []
    for nome_peca, limite in limites_padrao.items():
        chave_busca = _normalizar_texto(nome_peca)
        km_ultima_troca = 0.0

        for manutencao in manutencoes:
            servico = _normalizar_texto(manutencao.get('servico', ''))
            if chave_busca in servico:
                try:
                    km_ultima_troca = float(manutencao.get('km', 0) or 0)
                except (TypeError, ValueError):
                    km_ultima_troca = 0.0
                break

        km_rodado = max(0.0, km_atual_float - km_ultima_troca)
        km_restante = max(0.0, float(limite) - km_rodado)
        porcentagem = min(100.0, (km_rodado / float(limite)) * 100.0)

        pecas_dinamicas.append({
            'nome_peca': nome_peca,
            'km_rodado': int(round(km_rodado)),
            'km_restante': int(round(km_restante)),
            'porcentagem': porcentagem,
        })

    return pecas_dinamicas


def buscar_fipe(codigo_fipe, ano_modelo):
    codigo = str(codigo_fipe or '').strip()
    if not codigo:
        return None

    try:
        ano_int = int(str(ano_modelo or '').strip())
    except (TypeError, ValueError):
        return None

    url = f'https://brasilapi.com.br/api/fipe/preco/v1/{codigo}'

    try:
        resposta = requests.get(url, timeout=10)
        resposta.raise_for_status()
        dados = resposta.json()
    except requests.RequestException as exc:
        logger.warning('Falha ao consultar Brasil API para codigo FIPE %s: %s', codigo, exc)
        return None
    except ValueError as exc:
        logger.warning('Resposta JSON invalida na Brasil API para codigo FIPE %s: %s', codigo, exc)
        return None

    if not isinstance(dados, list):
        return None

    for item in dados:
        if not isinstance(item, dict):
            continue

        ano_api = item.get('anoModelo')
        try:
            if int(ano_api) == ano_int:
                return item
        except (TypeError, ValueError):
            continue

    return None


def _coletar_dados_exportacao(veiculo_id):
    db = firestore.client()

    doc_veiculo = db.collection('veiculos').document(str(veiculo_id)).get()
    veiculo = doc_veiculo.to_dict() if doc_veiculo.exists else {}

    abastecimentos = []
    for doc in db.collection('abastecimentos').order_by('km', direction=firestore.Query.DESCENDING).stream():
        dado = doc.to_dict() or {}
        if dado.get('veiculo_id') == veiculo_id:
            abastecimentos.append(dado)

    manutencoes = []
    for doc in db.collection('manutencoes').order_by('km', direction=firestore.Query.DESCENDING).stream():
        dado = doc.to_dict() or {}
        if dado.get('veiculo_id') == veiculo_id:
            manutencoes.append(dado)

    return veiculo, abastecimentos, manutencoes


def exportar_excel(veiculo_id):
    veiculo, abastecimentos, manutencoes = _coletar_dados_exportacao(veiculo_id)

    linhas = [
        {
            'Data': item.get('data', ''),
            'Tipo': 'Abastecimento',
            'Descricao': 'Combustivel',
            'KM Atual': float(item.get('km', 0) or 0),
            'Litros': float(item.get('litros', 0) or 0),
            'Valor Total (R$)': float(item.get('preco_total', item.get('valor', 0)) or 0),
            'Observacao': item.get('obs', ''),
        }
        for item in abastecimentos
    ]

    linhas.extend([
        {
            'Data': item.get('data', ''),
            'Tipo': 'Manutencao',
            'Descricao': item.get('servico', 'Servico'),
            'KM Atual': float(item.get('km', 0) or 0),
            'Litros': '',
            'Valor Total (R$)': float(item.get('valor', 0) or 0),
            'Observacao': item.get('obs', ''),
        }
        for item in manutencoes
    ])

    df_historico = pd.DataFrame(linhas)

    df_resumo = pd.DataFrame([
        {
            'Apelido': veiculo.get('apelido', 'Minha Moto'),
            'Marca': veiculo.get('marca', ''),
            'Modelo': veiculo.get('modelo', ''),
            'Ano': veiculo.get('ano_modelo', veiculo.get('ano', '')),
            'Total Gastos (R$)': 0 if df_historico.empty else round(df_historico['Valor Total (R$)'].sum(), 2),
        }
    ])

    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_resumo.to_excel(writer, sheet_name='Resumo', index=False)
        df_historico.to_excel(writer, sheet_name='Historico', index=False)

    output.seek(0)
    return output


def exportar_pdf(veiculo_id):
    veiculo, abastecimentos, manutencoes = _coletar_dados_exportacao(veiculo_id)

    linhas = []
    total_gastos = 0.0

    for item in abastecimentos:
        valor = float(item.get('preco_total', item.get('valor', 0)) or 0)
        total_gastos += valor
        linhas.append([
            str(item.get('data', '')),
            'Abastecimento',
            f"{float(item.get('km', 0) or 0):.0f}",
            f"R$ {valor:.2f}",
        ])

    for item in manutencoes:
        valor = float(item.get('valor', 0) or 0)
        total_gastos += valor
        linhas.append([
            str(item.get('data', '')),
            str(item.get('servico', 'Manutencao')),
            f"{float(item.get('km', 0) or 0):.0f}",
            f"R$ {valor:.2f}",
        ])

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    pdf.set_font('Helvetica', 'B', 14)
    pdf.cell(0, 10, 'Relatorio de Manutencao - Moto Tracker', ln=True)

    pdf.set_font('Helvetica', '', 10)
    marca = veiculo.get('marca', 'N/A')
    modelo = veiculo.get('modelo', 'N/A')
    ano = veiculo.get('ano_modelo', veiculo.get('ano', 'N/A'))
    pdf.cell(0, 8, f'Veiculo: {marca} {modelo} ({ano})', ln=True)
    pdf.ln(2)

    pdf.set_font('Helvetica', 'B', 10)
    pdf.cell(44, 8, 'Data', 1)
    pdf.cell(76, 8, 'Servico', 1)
    pdf.cell(28, 8, 'KM Atual', 1)
    pdf.cell(42, 8, 'Valor', 1, ln=True)

    pdf.set_font('Helvetica', '', 9)
    for linha in linhas:
        pdf.cell(44, 8, linha[0][:20], 1)
        pdf.cell(76, 8, linha[1][:33], 1)
        pdf.cell(28, 8, linha[2], 1)
        pdf.cell(42, 8, linha[3], 1, ln=True)

    pdf.ln(4)
    pdf.set_font('Helvetica', 'B', 11)
    pdf.cell(0, 8, f'Total acumulado de gastos: R$ {total_gastos:.2f}', ln=True)

    conteudo_pdf = pdf.output(dest='S')
    if isinstance(conteudo_pdf, str):
        conteudo_pdf = conteudo_pdf.encode('latin-1', errors='replace')

    output = BytesIO(conteudo_pdf)
    output.seek(0)
    return output
