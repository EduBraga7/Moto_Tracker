import functools
import os
import json
import math
import logging
import re
import requests
import unicodedata
from flask import Flask, render_template, request, redirect, url_for, session, flash
from flask import send_file
from datetime import datetime, timedelta
from dotenv import load_dotenv
from models import SaudePeca
from services import (
    processar_abastecimento,
    montar_payload_abastecimento,
    montar_payload_manutencao,
    montar_payload_veiculo,
    calcular_estatisticas_rodagem,
    calcular_pecas_monitoradas,
    calcular_desgaste_pecas,
    converter_data_padrao_para_iso,
    converter_data_iso_para_padrao,
    buscar_fipe,
    exportar_excel,
    exportar_pdf,
)

# --- IMPORTAÇÕES DO FIREBASE ---
import firebase_admin
from firebase_admin import credentials, firestore

# --- CONFIGURAÇÃO INICIAL ---
load_dotenv()
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'segredo_da_bros_2014') # Chave para criptografar o login
SENHA_SECRETA = os.getenv('APP_PASSWORD', 'admin')

# --- CONEXÃO COM O FIREBASE (VERSÃO SEGURA PARA DEPLOY) ---
if not firebase_admin._apps:
    firebase_key = os.getenv('FIREBASE_KEY')
    if not firebase_key:
        raise RuntimeError('Variavel de ambiente FIREBASE_KEY ausente.')

    try:
        key_dict = json.loads(firebase_key)
        cred = credentials.Certificate(key_dict)
        firebase_admin.initialize_app(cred)
    except Exception as exc:
        logging.exception('Falha ao inicializar Firebase com FIREBASE_KEY: %s', exc)
        raise RuntimeError('FIREBASE_KEY invalida para inicializacao do Firebase.') from exc

db = firestore.client()


def _existe_registro_legado_sem_veiculo(nome_colecao):
    try:
        for doc in db.collection(nome_colecao).stream():
            dado = doc.to_dict() or {}
            if not dado.get('veiculo_id'):
                return True
    except Exception as exc:
        logging.exception('Erro ao verificar legados em %s: %s', nome_colecao, exc)
    return False


def criar_veiculo_padrao():
    ref = db.collection('veiculos').document()
    payload = montar_payload_veiculo(
        id_veiculo=ref.id,
        apelido='Minha Moto',
        fipe_codigo='N/A',
        ano_modelo=0,
        valor_fipe='N/A',
        km_atual=0,
    )
    payload['data_cadastro'] = datetime.now().strftime('%d/%m/%Y %H:%M')
    ref.set(payload)

    return payload


def obter_primeiro_veiculo():
    try:
        docs = db.collection('veiculos').limit(1).stream()
        for doc in docs:
            dado = doc.to_dict() or {}
            dado['id'] = doc.id
            return dado
    except Exception as exc:
        logging.exception('Erro ao obter primeiro veiculo: %s', exc)
    return None


def _garantir_veiculo_para_historico_legado():
    if primeiro_veiculo := obter_primeiro_veiculo():
        return primeiro_veiculo

    existe_legado = _existe_registro_legado_sem_veiculo('abastecimentos') or _existe_registro_legado_sem_veiculo('manutencoes')
    if not existe_legado:
        return None

    try:
        return criar_veiculo_padrao()
    except Exception as exc:
        logging.exception('Erro ao criar veiculo padrao para migracao: %s', exc)
        return None


def obter_veiculo_ativo():
    if veiculo_ativo_id := session.get('veiculo_ativo_id'):
        try:
            doc = db.collection('veiculos').document(veiculo_ativo_id).get()
            if doc.exists:
                dado = doc.to_dict() or {}
                dado['id'] = doc.id
                return dado
        except Exception as exc:
            logging.exception('Erro ao obter veiculo ativo da sessao: %s', exc)

    if primeiro_veiculo := _garantir_veiculo_para_historico_legado():
        session['veiculo_ativo_id'] = primeiro_veiculo['id']
    return primeiro_veiculo


def migrar_registros_sem_veiculo_id(nome_colecao, veiculo_id):
    if not veiculo_id:
        return

    chave_migracao = f'migracao_{nome_colecao}_{veiculo_id}'
    if session.get(chave_migracao):
        return

    try:
        for doc in db.collection(nome_colecao).stream():
            dado = doc.to_dict() or {}
            if not dado.get('veiculo_id'):
                db.collection(nome_colecao).document(doc.id).update({'veiculo_id': veiculo_id})
    except Exception as exc:
        logging.exception('Erro ao migrar registros legados da colecao %s: %s', nome_colecao, exc)

    session[chave_migracao] = True


def registro_pertence_ao_veiculo(nome_colecao, registro_id, veiculo_id):
    if not veiculo_id:
        return False

    try:
        doc = db.collection(nome_colecao).document(registro_id).get()
        if not doc.exists:
            return False
        return (doc.to_dict() or {}).get('veiculo_id') == veiculo_id
    except Exception as exc:
        logging.exception('Erro ao validar ownership em %s id=%s: %s', nome_colecao, registro_id, exc)
        return False


def listar_veiculos():
    lista_veiculos = []
    try:
        veiculos_ref = db.collection('veiculos').stream()
        for doc in veiculos_ref:
            dado = doc.to_dict() or {}
            dado['id'] = doc.id
            lista_veiculos.append(dado)
    except Exception as exc:
        logging.exception('Erro ao listar veiculos: %s', exc)

    return lista_veiculos


def _converter_float(valor):
    try:
        return float(valor or 0)
    except (TypeError, ValueError):
        return 0.0


def _converter_int_nao_negativo(valor):
    texto = str(valor or '').strip()

    # Aceita formato de milhar em pt-BR/en-US para odometro (ex: 60.000, 60,000).
    if texto and re.fullmatch(r'\d{1,3}([.,]\d{3})+', texto):
        try:
            return max(0, int(texto.replace('.', '').replace(',', '')))
        except (TypeError, ValueError):
            return 0

    try:
        return max(0, int(float(texto or 0)))
    except (TypeError, ValueError):
        return 0


def _parse_data_registro(data_texto):
    texto = str(data_texto or '').strip()
    for formato in ('%d/%m/%Y %H:%M', '%d/%m/%Y'):
        try:
            return datetime.strptime(texto, formato)
        except ValueError:
            continue
    return None


def _formatar_moeda_br(valor):
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def _formatar_km_br(valor):
    return f"{int(valor):,}".replace(',', '.')


def _coletar_km_maximo_por_veiculo(lista_veiculos):
    veiculos_ids = {str(veiculo.get('id', '')).strip() for veiculo in lista_veiculos if veiculo.get('id')}
    km_por_veiculo = {veiculo_id: 0 for veiculo_id in veiculos_ids}

    if not veiculos_ids:
        return km_por_veiculo

    for doc in db.collection('abastecimentos').stream():
        dado = doc.to_dict() or {}
        veiculo_id = str(dado.get('veiculo_id', '')).strip()
        if veiculo_id not in km_por_veiculo:
            continue

        km_registro = _converter_int_nao_negativo(dado.get('km', 0))
        if km_registro > km_por_veiculo[veiculo_id]:
            km_por_veiculo[veiculo_id] = km_registro

    return km_por_veiculo


def _sincronizar_km_atual_veiculo(veiculo_id, km_candidato):
    km_novo = _converter_int_nao_negativo(km_candidato)
    if not veiculo_id:
        return km_novo

    try:
        doc_ref = db.collection('veiculos').document(veiculo_id)
        doc = doc_ref.get()
        if not doc.exists:
            return km_novo

        km_atual = _converter_int_nao_negativo((doc.to_dict() or {}).get('km_atual', 0))
        if km_novo > km_atual:
            doc_ref.update({'km_atual': km_novo})
            return km_novo
        return km_atual
    except Exception as exc:
        logging.exception('Erro ao sincronizar km_atual do veiculo id=%s: %s', veiculo_id, exc)
        return km_novo


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

    for doc in db.collection('abastecimentos').stream():
        dado = doc.to_dict() or {}
        veiculo_id = str(dado.get('veiculo_id', '')).strip()
        if veiculo_id not in metricas:
            continue

        valor = _converter_float(dado.get('preco_total', dado.get('valor', 0)))
        metricas[veiculo_id]['total_investido'] += valor

        data_registro = _parse_data_registro(dado.get('data', ''))
        if data_registro and data_registro >= limite_30_dias:
            metricas[veiculo_id]['custo_ultimos_30_dias'] += valor

    for doc in db.collection('manutencoes').stream():
        dado = doc.to_dict() or {}
        veiculo_id = str(dado.get('veiculo_id', '')).strip()
        if veiculo_id not in metricas:
            continue

        valor = _converter_float(dado.get('valor', 0))
        metricas[veiculo_id]['total_investido'] += valor

        data_registro = _parse_data_registro(dado.get('data', ''))
        if data_registro and data_registro >= limite_30_dias:
            metricas[veiculo_id]['custo_ultimos_30_dias'] += valor

    return metricas


def _enriquecer_veiculos_com_metricas(lista_veiculos):
    metricas_por_veiculo = _coletar_metricas_por_veiculo(lista_veiculos)
    km_por_veiculo = _coletar_km_maximo_por_veiculo(lista_veiculos)

    for veiculo in lista_veiculos:
        veiculo_id = str(veiculo.get('id', '')).strip()
        metricas = metricas_por_veiculo.get(veiculo_id, {})
        total_investido = _converter_float(metricas.get('total_investido', 0))
        custo_ultimos_30_dias = _converter_float(metricas.get('custo_ultimos_30_dias', 0))
        km_atual = _converter_int_nao_negativo(veiculo.get('km_atual', 0))
        km_detectado = _converter_int_nao_negativo(km_por_veiculo.get(veiculo_id, 0))

        if km_detectado > km_atual:
            km_atual = _sincronizar_km_atual_veiculo(veiculo_id, km_detectado)

        veiculo['total_investido'] = round(total_investido, 2)
        veiculo['total_investido_fmt'] = _formatar_moeda_br(total_investido)
        veiculo['custo_medio_mes'] = round(custo_ultimos_30_dias, 2)
        veiculo['custo_medio_mes_fmt'] = _formatar_moeda_br(custo_ultimos_30_dias)
        veiculo['km_atual'] = km_atual
        veiculo['km_atual_fmt'] = _formatar_km_br(km_atual)
        
        # Calcula saúde do óleo (0-1000 KM)
        ultimo_oleo_km = _converter_int_nao_negativo(veiculo.get('ultimo_oleo_km', 0))
        km_desde_oleo = max(0, km_atual - ultimo_oleo_km)
        veiculo['km_desde_oleo'] = km_desde_oleo
        
        # Determina status do óleo
        if km_desde_oleo <= 700:
            veiculo['status_oleo'] = 'verde'
        elif km_desde_oleo <= 900:
            veiculo['status_oleo'] = 'amarelo'
        else:
            veiculo['status_oleo'] = 'vermelho'


def _normalizar_texto(texto):
    base = unicodedata.normalize('NFKD', str(texto or '').lower())
    return ''.join(ch for ch in base if not unicodedata.combining(ch)).strip()


LIMITE_PADRAO_PECAS = {
    'oleo': ('Óleo', 1000),
    'relacao': ('Relação', 15000),
    'pneu': ('Pneus', 12000),
}


def _obter_limite_padrao_peca(nome_peca):
    nome_norm = _normalizar_texto(nome_peca)
    for chave_norm, (nome_canonico, limite) in LIMITE_PADRAO_PECAS.items():
        if chave_norm in nome_norm or nome_norm in chave_norm:
            return nome_canonico, int(limite)
    return str(nome_peca or 'Peça').strip() or 'Peça', 1000


def _calcular_percentual_peca(km_atual, km_ultima_troca, km_limite):
    km_limite_int = max(1, _converter_int_nao_negativo(km_limite))
    km_rodado = max(0, _converter_int_nao_negativo(km_atual) - _converter_int_nao_negativo(km_ultima_troca))
    # Formula generica solicitada: P = ((KM_atual - KM_ultima_troca) / KM_limite) * 100
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


def _mapa_limites_configuracao(lista_config):
    limites = {}
    for item in (lista_config or []):
        nome = str(item.get('nome', '')).strip()
        if not nome:
            continue
        limite = _converter_int_nao_negativo(item.get('km_vida_util', item.get('km', 0)))
        if limite <= 0:
            continue
        limites[_normalizar_texto(nome)] = {
            'nome_peca': nome,
            'km_limite': limite,
        }
    return limites


def _upsert_saude_peca(veiculo_id, nome_peca, ultimo_km_troca=None, km_limite=None):
    if not veiculo_id:
        return

    nome_canonico, limite_padrao = _obter_limite_padrao_peca(nome_peca)
    doc_id = f"{veiculo_id}__{_normalizar_texto(nome_canonico)}"
    doc_ref = db.collection('saude_pecas').document(doc_id)
    doc = doc_ref.get()
    atual = doc.to_dict() or {}

    payload = {
        'veiculo_id': veiculo_id,
        'nome_peca': nome_canonico,
        'ultimo_km_troca': _converter_int_nao_negativo(atual.get('ultimo_km_troca', 0)),
        'km_limite': _converter_int_nao_negativo(atual.get('km_limite', limite_padrao)) or limite_padrao,
    }

    if ultimo_km_troca is not None:
        payload['ultimo_km_troca'] = _converter_int_nao_negativo(ultimo_km_troca)
    if km_limite is not None:
        payload['km_limite'] = max(1, _converter_int_nao_negativo(km_limite))

    saude_peca = SaudePeca(
        veiculo_id=payload['veiculo_id'],
        nome_peca=payload['nome_peca'],
        ultimo_km_troca=payload['ultimo_km_troca'],
        km_limite=payload['km_limite'],
    )
    doc_ref.set(saude_peca.to_firestore(), merge=True)


def _listar_saude_pecas(veiculo, veiculo_id, lista_config=None):
    if not veiculo or not veiculo_id:
        return []

    km_atual = _converter_int_nao_negativo(veiculo.get('km_atual', 0))
    limites_config = _mapa_limites_configuracao(lista_config)
    docs_existentes = {}

    for doc in db.collection('saude_pecas').where('veiculo_id', '==', veiculo_id).stream():
        dado = doc.to_dict() or {}
        chave = _normalizar_texto(dado.get('nome_peca', ''))
        if chave:
            docs_existentes[chave] = dado

    pecas_base = []
    for _, (nome_padrao, limite_padrao) in LIMITE_PADRAO_PECAS.items():
        pecas_base.append({'nome_peca': nome_padrao, 'km_limite': limite_padrao})
    for _, item in limites_config.items():
        pecas_base.append({'nome_peca': item['nome_peca'], 'km_limite': item['km_limite']})

    saude_pecas = []
    chaves_vistas = set()
    apelido_norm = _normalizar_texto(veiculo.get('apelido', ''))
    modelo_norm = _normalizar_texto(veiculo.get('modelo', ''))

    for item in pecas_base:
        nome_peca = item['nome_peca']
        chave = _normalizar_texto(nome_peca)
        if chave in chaves_vistas:
            continue
        chaves_vistas.add(chave)

        dado = docs_existentes.get(chave, {})
        limite = _converter_int_nao_negativo(dado.get('km_limite', item['km_limite'])) or item['km_limite']
        ultimo = _converter_int_nao_negativo(dado.get('ultimo_km_troca', 0))

        if chave == 'oleo' and ultimo <= 0:
            legado_oleo = _converter_int_nao_negativo(veiculo.get('ultimo_oleo_km', 0))
            if legado_oleo > 0:
                ultimo = legado_oleo
            elif 'bros' in apelido_norm or 'bros' in modelo_norm:
                ultimo = 64913

        if not dado:
            _upsert_saude_peca(veiculo_id, nome_peca, ultimo_km_troca=ultimo, km_limite=limite)
        elif limite != _converter_int_nao_negativo(dado.get('km_limite', 0)):
            _upsert_saude_peca(veiculo_id, nome_peca, km_limite=limite)

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


def send_telegram_message(chat_id, text):
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    if not bot_token:
        logging.error('TELEGRAM_BOT_TOKEN nao configurado para responder mensagens.')
        return

    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    try:
        requests.post(url, json={'chat_id': chat_id, 'text': text}, timeout=10)
    except Exception as exc:
        logging.exception('Erro ao responder mensagem no Telegram: %s', exc)


def obter_icone_peca(nome_peca):
    texto = _normalizar_texto(nome_peca)

    mapa_icones = (
        ('oleo', 'fa-solid fa-oil-can'),
        ('pneu', 'fa-solid fa-circle-notch'),
        ('relacao', 'fa-solid fa-link'),
    )

    return next((icone for chave, icone in mapa_icones if chave in texto), 'fa-solid fa-gear')

# --- DECORATOR: O "PORTEIRO" ---
def login_required(f):
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logado' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# ================= ROTAS DE LOGIN =================
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        senha_digitada = request.form.get('senha')
        if senha_digitada == SENHA_SECRETA:
            session['logado'] = True
            return redirect(url_for('index'))
        else:
            flash('Senha incorreta!')
            return redirect(url_for('login'))
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('logado', None)
    return redirect(url_for('login'))

# ================= ROTA PRINCIPAL =================
@app.route('/')
@login_required 
def index():
    veiculo_ativo = obter_veiculo_ativo()
    veiculo_ativo_id = veiculo_ativo.get('id') if veiculo_ativo else None

    if veiculo_ativo_id:
        migrar_registros_sem_veiculo_id('abastecimentos', veiculo_ativo_id)
        migrar_registros_sem_veiculo_id('manutencoes', veiculo_ativo_id)

    abastecimentos_ref = db.collection('abastecimentos').order_by('km', direction=firestore.Query.DESCENDING).stream()
    lista_abastecimentos = []
    
    for doc in abastecimentos_ref:
        dado = doc.to_dict()
        if not veiculo_ativo_id or dado.get('veiculo_id') != veiculo_ativo_id:
            continue
        dado['id'] = doc.id
        dado['km'] = float(dado.get('km', 0))
        dado['litros'] = float(dado.get('litros', 0))
        dado['preco_total'] = float(dado.get('preco_total', dado.get('valor', 0)))
        lista_abastecimentos.append(dado)

    manutencoes_ref = db.collection('manutencoes').order_by('km', direction=firestore.Query.DESCENDING).stream()
    lista_manutencoes = []
    for doc in manutencoes_ref:
        dado = doc.to_dict()
        if not veiculo_ativo_id or dado.get('veiculo_id') != veiculo_ativo_id:
            continue
        dado['id'] = doc.id
        dado['valor'] = float(dado.get('valor', 0))
        lista_manutencoes.append(dado)

    config_ref = db.collection('configuracoes').stream()
    lista_config = []
    for doc in config_ref:
        dado = doc.to_dict()
        dado['id'] = doc.id
        lista_config.append(dado)

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
            atual = lista_invertida[i+1]
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

    km_atual_moto = _converter_int_nao_negativo(veiculo_ativo.get('km_atual', 0) if veiculo_ativo else 0)
    km_detectado_abastecimentos = _converter_int_nao_negativo(max((a.get('km', 0) for a in lista_abastecimentos), default=0))
    if veiculo_ativo_id and km_detectado_abastecimentos > km_atual_moto:
        km_atual_moto = _sincronizar_km_atual_veiculo(veiculo_ativo_id, km_detectado_abastecimentos)
        if veiculo_ativo:
            veiculo_ativo['km_atual'] = km_atual_moto

    saude_pecas = []
    if veiculo_ativo:
        veiculo_ativo['km_atual'] = km_atual_moto
        saude_pecas = _listar_saude_pecas(veiculo_ativo, veiculo_ativo_id, lista_config)
        saude_oleo = next((item for item in saude_pecas if _normalizar_texto(item.get('nome_peca', '')) == 'oleo'), None)
        if saude_oleo:
            veiculo_ativo.update({
                'km_rodado': saude_oleo['km_rodado'],
                'km_restante': saude_oleo['km_restante'],
                'km_rodado_oleo': saude_oleo['km_rodado'],
                'km_restante_oleo': saude_oleo['km_restante'],
                'oleo_percentual': saude_oleo['porcentagem'],
                'status_oleo': saude_oleo['status'],
            })

    pecas_monitoradas = calcular_pecas_monitoradas(lista_config, lista_manutencoes, km_atual_moto)
    pecas_dinamicas = calcular_desgaste_pecas(km_atual_moto, lista_manutencoes)

    abastecimentos_exibicao = []
    for i in range(len(lista_abastecimentos)):
        item = lista_abastecimentos[i].copy()
        item['kml'] = "---"
        item['preco_litro'] = round(item['preco_total'] / item['litros'], 2) if item['litros'] > 0 else 0
        item['data_iso_edit'] = converter_data_padrao_para_iso(item.get('data', ''))
        if i < len(lista_abastecimentos) - 1:
            anterior = lista_abastecimentos[i+1]
            diff = item['km'] - anterior['km']
            if diff > 0 and item['litros'] > 0:
                calc = diff / item['litros']
                item['kml'] = f"{calc:.1f}"
        abastecimentos_exibicao.append(item)

    return render_template('index.html', abastecimentos=abastecimentos_exibicao, manutencoes=lista_manutencoes, configuracoes=lista_config, pecas_monitoradas=pecas_monitoradas, pecas_dinamicas=pecas_dinamicas, saude_pecas=saude_pecas, kpi_media=f"{media_geral:.1f}", kpi_custo_km=f"{custo_km:.2f}", kpi_total=f"{total_geral:.2f}", labels_linha=labels_linha, values_linha=values_linha, labels_barra=labels_barra, values_barra=values_barra, km_total_mes=estatisticas_rodagem['km_total_mes'], media_km_dia=estatisticas_rodagem['media_km_dia'], veiculo_ativo=veiculo_ativo)


@app.route('/exportar/excel')
@login_required
def exportar_excel_rota():
    veiculo_id = str(request.args.get('veiculo_id', '')).strip()
    veiculo_exportacao = None

    if veiculo_id:
        doc = db.collection('veiculos').document(veiculo_id).get()
        if not doc.exists:
            flash('Veiculo informado para exportacao nao foi encontrado.', 'error')
            return redirect(url_for('veiculos'))
        veiculo_exportacao = doc.to_dict() or {}
        veiculo_exportacao['id'] = doc.id
    else:
        veiculo_exportacao = obter_veiculo_ativo()

    if not veiculo_exportacao:
        flash('Nenhum veiculo disponivel para exportar.', 'error')
        return redirect(url_for('veiculos'))

    veiculo_id = veiculo_exportacao.get('id')
    arquivo_excel = exportar_excel(veiculo_id)
    nome_base = str(veiculo_exportacao.get('apelido', 'veiculo')).replace(' ', '_').lower()
    nome_arquivo = f'relatorio_{nome_base}.xlsx'

    return send_file(
        arquivo_excel,
        as_attachment=True,
        download_name=nome_arquivo,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )


@app.route('/exportar/pdf')
@login_required
def exportar_pdf_rota():
    veiculo_id = str(request.args.get('veiculo_id', '')).strip()
    veiculo_exportacao = None

    if veiculo_id:
        doc = db.collection('veiculos').document(veiculo_id).get()
        if not doc.exists:
            flash('Veiculo informado para exportacao nao foi encontrado.', 'error')
            return redirect(url_for('veiculos'))
        veiculo_exportacao = doc.to_dict() or {}
        veiculo_exportacao['id'] = doc.id
    else:
        veiculo_exportacao = obter_veiculo_ativo()

    if not veiculo_exportacao:
        flash('Nenhum veiculo disponivel para exportar.', 'error')
        return redirect(url_for('veiculos'))

    veiculo_id = veiculo_exportacao.get('id')
    arquivo_pdf = exportar_pdf(veiculo_id)
    nome_base = str(veiculo_exportacao.get('apelido', 'veiculo')).replace(' ', '_').lower()
    nome_arquivo = f'relatorio_{nome_base}.pdf'

    return send_file(
        arquivo_pdf,
        as_attachment=True,
        download_name=nome_arquivo,
        mimetype='application/pdf',
    )

# ================= ROTAS DE AÇÃO =================

@app.route('/adicionar_rapido', methods=['POST'])
@login_required
def adicionar_rapido():
    texto = request.form.get('smart_text', '').strip()
    km_atual_texto = request.form.get('km_atual', '').strip()
    veiculo_ativo = obter_veiculo_ativo()
    if not veiculo_ativo:
        flash('Cadastre um veiculo antes de adicionar abastecimentos.', 'error')
        return redirect(url_for('pecas') + '#AbaVeiculos')

    veiculo_ativo_id = veiculo_ativo['id']

    try:
        if not texto:
            flash('Informe valor, litros e KM para registrar o abastecimento.', 'error')
            return redirect('/')

        if not km_atual_texto:
            flash('Informe o KM atual para sincronizar o hodometro da moto.', 'error')
            return redirect('/')

        km_informado = _converter_int_nao_negativo(km_atual_texto)
        if km_informado <= 0:
            flash('KM atual invalido para o abastecimento.', 'error')
            return redirect('/')

        ultimo_km_registrado = None
        docs = db.collection('abastecimentos').order_by('km', direction=firestore.Query.DESCENDING).stream()
        for doc in docs:
            dado = doc.to_dict() or {}
            if dado.get('veiculo_id') == veiculo_ativo_id:
                ultimo_km_registrado = dado.get('km', 0)
                break

        novo_abastecimento = processar_abastecimento(texto, ultimo_km_registrado, veiculo_ativo_id)
        if novo_abastecimento is None:
            flash('Formato invalido. Use: valor litros km.', 'error')
            return redirect('/')

        payload = montar_payload_abastecimento(
            km=km_informado,
            litros=novo_abastecimento['litros'],
            preco_total=novo_abastecimento['preco_total'],
            data_registro=novo_abastecimento['data'],
            veiculo_id=veiculo_ativo_id,
        )
        km_veiculo_atual = _converter_int_nao_negativo(veiculo_ativo.get('km_atual', 0))
        db.collection('abastecimentos').add(payload)

        if payload['km'] > km_veiculo_atual:
            km_sincronizado = _sincronizar_km_atual_veiculo(veiculo_ativo_id, payload['km'])
            flash(f'Abastecimento salvo com sucesso. Hodômetro da moto atualizado para {km_sincronizado} KM e Saúde das Peças recalculada.', 'success')
        else:
            flash('Abastecimento salvo com sucesso. Hodômetro da moto já estava sincronizado e Saúde das Peças recalculada.', 'success')
    except Exception as exc:
        logging.exception('Erro ao adicionar abastecimento rapido: %s', exc)
        flash('Nao foi possivel salvar o abastecimento.', 'error')
    return redirect('/')

@app.route('/adicionar_manutencao', methods=['POST'])
@login_required
def adicionar_manutencao():
    veiculo_ativo = obter_veiculo_ativo()
    if not veiculo_ativo:
        flash('Cadastre um veiculo antes de registrar manutencoes.', 'error')
        return redirect(url_for('pecas') + '#AbaVeiculos')

    try:
        peca = request.form.get('peca', '').strip()
        servico_form = request.form.get('servico', '').strip()
        servico = servico_form or (f'Troca de {peca}' if peca else 'Troca')

        data_form = request.form.get('data', '').strip()
        data_padrao = converter_data_iso_para_padrao(data_form) if data_form else datetime.now().strftime('%d/%m/%Y %H:%M')

        payload = montar_payload_manutencao(
            km=request.form['km'],
            servico=servico,
            valor=request.form['valor'],
            data_padrao=data_padrao,
            obs=request.form.get('obs', ''),
            veiculo_id=veiculo_ativo['id'],
        )

        db.collection('manutencoes').add(payload)

        peca_norm = _normalizar_texto(peca)
        if peca_norm:
            km_troca = _converter_int_nao_negativo(payload.get('km', 0))
            _upsert_saude_peca(veiculo_ativo['id'], peca, ultimo_km_troca=km_troca)

            # Compatibilidade legada para o campo antigo de óleo.
            if peca_norm == 'oleo':
                db.collection('veiculos').document(veiculo_ativo['id']).update({'ultimo_oleo_km': km_troca})
    except Exception as exc:
        logging.exception('Erro ao adicionar manutencao: %s', exc)
    return redirect(url_for('garagem'))

@app.route('/salvar_config', methods=['POST'])
@login_required
def salvar_config():
    try:
        nome = request.form.get('nome', '').strip()
        limite = float(request.form.get('km', 0) or 0)
        db.collection('configuracoes').add({'nome': nome, 'km_vida_util': limite})

        veiculo_ativo = obter_veiculo_ativo()
        if veiculo_ativo and nome and limite > 0:
            _upsert_saude_peca(veiculo_ativo.get('id'), nome, km_limite=int(limite))
    except Exception as exc:
        logging.exception('Erro ao salvar configuracao: %s', exc)
    return redirect(url_for('pecas'))

@app.route('/deletar/<id>')
@login_required
def deletar(id):
    veiculo_ativo = obter_veiculo_ativo()
    veiculo_ativo_id = veiculo_ativo.get('id') if veiculo_ativo else None

    if not registro_pertence_ao_veiculo('abastecimentos', id, veiculo_ativo_id):
        flash('Registro de abastecimento nao pertence ao veiculo ativo.', 'error')
        return redirect('/')

    db.collection('abastecimentos').document(id).delete()
    return redirect('/')

@app.route('/deletar_manutencao/<id>')
@login_required
def deletar_manutencao(id):
    veiculo_ativo = obter_veiculo_ativo()
    veiculo_ativo_id = veiculo_ativo.get('id') if veiculo_ativo else None

    if not registro_pertence_ao_veiculo('manutencoes', id, veiculo_ativo_id):
        flash('Registro de manutencao nao pertence ao veiculo ativo.', 'error')
        return redirect(url_for('pecas'))

    db.collection('manutencoes').document(id).delete()
    return redirect('/')

@app.route('/deletar_config/<id>')
@login_required
def deletar_config(id):
    db.collection('configuracoes').document(id).delete()
    return redirect(url_for('pecas'))


@app.route('/atualizar_config/<id>', methods=['POST'])
@login_required
def atualizar_config(id):
    try:
        nome = request.form.get('nome', '').strip()
        limite_km = float(request.form.get('km_vida_util', 0) or 0)

        payload = {
            'km_vida_util': limite_km,
            'km': limite_km,
        }
        if nome:
            payload['nome'] = nome

        db.collection('configuracoes').document(id).update(payload)

        veiculo_ativo = obter_veiculo_ativo()
        if veiculo_ativo and nome and limite_km > 0:
            _upsert_saude_peca(veiculo_ativo.get('id'), nome, km_limite=int(limite_km))
    except Exception as exc:
        logging.exception('Erro ao atualizar configuracao id=%s: %s', id, exc)

    return redirect(url_for('pecas'))


@app.route('/peca/<path:nome_peca>')
@login_required
def detalhes_peca(nome_peca):
    veiculo_ativo = obter_veiculo_ativo()
    veiculo_ativo_id = veiculo_ativo.get('id') if veiculo_ativo else None
    nome_peca_normalizado = _normalizar_texto(nome_peca)

    manutencoes_peca = []
    if veiculo_ativo_id:
        manutencoes_ref = db.collection('manutencoes').order_by('km', direction=firestore.Query.DESCENDING).stream()
        filtro_peca = nome_peca_normalizado
        for doc in manutencoes_ref:
            dado = doc.to_dict() or {}
            if dado.get('veiculo_id') != veiculo_ativo_id:
                continue
            if filtro_peca not in _normalizar_texto(dado.get('servico', '')):
                continue
            dado['id'] = doc.id
            dado['valor'] = float(dado.get('valor', 0))
            dado['km'] = _converter_int_nao_negativo(dado.get('km', 0))
            manutencoes_peca.append(dado)

    km_rodado = 0
    km_restante = 1000
    km_limite = 1000
    oleo_percentual = 0
    status_oleo = 'verde'
    km_atual_veiculo = _converter_int_nao_negativo(veiculo_ativo.get('km_atual', 0) if veiculo_ativo else 0)
    media_km_dia = 0.0
    proxima_troca_data = 'Sem dados suficientes'
    total_investido_peca = round(sum(_converter_float(item.get('valor', 0)) for item in manutencoes_peca), 2)
    total_investido_peca_fmt = _formatar_moeda_br(total_investido_peca)
    especificacao_peca = '10W-30 / 1 Litro' if nome_peca_normalizado == 'oleo' else 'Conforme manual da peça'
    ciclos_labels = []
    ciclos_values = []
    manual_bros_pdf_url = os.getenv('BROS_MANUAL_PDF_URL', 'https://www.manualslib.com/download/1179286/Honda-Nxr150-Bros.pdf')

    saude_pecas = _listar_saude_pecas(veiculo_ativo, veiculo_ativo_id) if veiculo_ativo else []
    saude_peca_atual = next(
        (item for item in saude_pecas if _normalizar_texto(item.get('nome_peca', '')) == nome_peca_normalizado),
        None,
    )
    if saude_peca_atual:
        km_rodado = saude_peca_atual['km_rodado']
        km_restante = saude_peca_atual['km_restante']
        km_limite = saude_peca_atual['km_limite']
        oleo_percentual = saude_peca_atual['porcentagem']
        status_oleo = saude_peca_atual['status']

    if nome_peca_normalizado == 'oleo' and veiculo_ativo:

        lista_abastecimentos = []
        abastecimentos_ref = db.collection('abastecimentos').order_by('km', direction=firestore.Query.DESCENDING).stream()
        for doc in abastecimentos_ref:
            dado = doc.to_dict() or {}
            if dado.get('veiculo_id') != veiculo_ativo_id:
                continue
            lista_abastecimentos.append({
                'km': _converter_float(dado.get('km', 0)),
                'data': dado.get('data', ''),
            })

        estatisticas_rodagem = calcular_estatisticas_rodagem(lista_abastecimentos)
        media_km_dia = _converter_float(estatisticas_rodagem.get('media_km_dia', 0))

        if km_restante <= 0:
            proxima_troca_data = datetime.now().strftime('%d/%m/%Y')
        elif media_km_dia > 0:
            dias_para_troca = int(math.ceil(km_restante / media_km_dia))
            proxima_troca_data = (datetime.now() + timedelta(days=dias_para_troca)).strftime('%d/%m/%Y')

        trocas_oleo = []
        for item in manutencoes_peca:
            servico_norm = _normalizar_texto(item.get('servico', ''))
            if 'troca' in servico_norm:
                trocas_oleo.append(item)

        trocas_oleo.sort(key=lambda item: item.get('km', 0))
        for idx in range(1, len(trocas_oleo)):
            anterior = trocas_oleo[idx - 1]
            atual = trocas_oleo[idx]
            km_ciclo = max(0, _converter_int_nao_negativo(atual.get('km', 0)) - _converter_int_nao_negativo(anterior.get('km', 0)))
            if km_ciclo <= 0:
                continue
            data_atual = _parse_data_registro(atual.get('data', ''))
            label = data_atual.strftime('%d/%m') if data_atual else f'Ciclo {idx}'
            ciclos_labels.append(label)
            ciclos_values.append(km_ciclo)

        if not ciclos_values and km_rodado > 0:
            ciclos_labels = ['Atual']
            ciclos_values = [km_rodado]

        ciclos_labels = ciclos_labels[-5:]
        ciclos_values = ciclos_values[-5:]

    return render_template(
        'detalhes_peca.html',
        nome_peca=nome_peca,
        nome_peca_normalizado=nome_peca_normalizado,
        icone_peca=obter_icone_peca(nome_peca),
        manutencoes_peca=manutencoes_peca,
        km_rodado=km_rodado,
        km_restante=km_restante,
        km_limite=km_limite,
        km_rodado_oleo=km_rodado,
        km_restante_oleo=km_restante,
        km_atual_veiculo=km_atual_veiculo,
        oleo_percentual=oleo_percentual,
        status_oleo=status_oleo,
        media_km_dia=media_km_dia,
        proxima_troca_data=proxima_troca_data,
        total_investido_peca=total_investido_peca,
        total_investido_peca_fmt=total_investido_peca_fmt,
        especificacao_peca=especificacao_peca,
        ciclos_labels=json.dumps(ciclos_labels),
        ciclos_values=json.dumps(ciclos_values),
        manual_bros_pdf_url=manual_bros_pdf_url,
    )


@app.route('/oficina')
@app.route('/garagem')
@login_required
def garagem():
    veiculo_ativo = obter_veiculo_ativo()
    veiculo_ativo_id = veiculo_ativo.get('id') if veiculo_ativo else None
    peca_preselecionada = request.args.get('peca', '').strip()
    km_preselecionado = request.args.get('km', '').strip()

    if veiculo_ativo_id:
        migrar_registros_sem_veiculo_id('manutencoes', veiculo_ativo_id)

    manutencoes_ref = db.collection('manutencoes').order_by('km', direction=firestore.Query.DESCENDING).stream()
    lista_manutencoes = []
    for doc in manutencoes_ref:
        dado = doc.to_dict() or {}
        if not veiculo_ativo_id or dado.get('veiculo_id') != veiculo_ativo_id:
            continue
        dado['id'] = doc.id
        dado['valor'] = float(dado.get('valor', 0))
        lista_manutencoes.append(dado)

    km_atual_moto = _converter_int_nao_negativo(veiculo_ativo.get('km_atual', 0) if veiculo_ativo else 0)
    pecas_dinamicas = _listar_saude_pecas(veiculo_ativo, veiculo_ativo_id)
    for item in pecas_dinamicas:
        item['nome_peca'] = item.get('nome_peca', 'Peça')
        item['porcentagem'] = item.get('porcentagem', 0)
        item['km_rodado'] = item.get('km_rodado', 0)
        item['km_restante'] = item.get('km_restante', 0)

    return render_template(
        'garagem.html',
        manutencoes=lista_manutencoes,
        veiculo_ativo=veiculo_ativo,
        pecas_dinamicas=pecas_dinamicas,
        peca_preselecionada=peca_preselecionada,
        km_preselecionado=km_preselecionado,
    )


@app.route('/veiculos')
@login_required
def veiculos():
    veiculo_ativo = obter_veiculo_ativo()
    lista_veiculos = listar_veiculos()
    _enriquecer_veiculos_com_metricas(lista_veiculos)

    return render_template('veiculos.html', veiculos=lista_veiculos, veiculo_ativo=veiculo_ativo, veiculo_editando=None)


@app.route('/pecas')
@login_required
def pecas():
    veiculo_ativo = obter_veiculo_ativo()
    veiculo_ativo_id = veiculo_ativo.get('id') if veiculo_ativo else None

    if veiculo_ativo_id:
        migrar_registros_sem_veiculo_id('manutencoes', veiculo_ativo_id)

    manutencoes_ref = db.collection('manutencoes').order_by('km', direction=firestore.Query.DESCENDING).stream()
    lista_manutencoes = []
    for doc in manutencoes_ref:
        dado = doc.to_dict()
        if not veiculo_ativo_id or dado.get('veiculo_id') != veiculo_ativo_id:
            continue
        dado['id'] = doc.id
        dado['valor'] = float(dado.get('valor', 0))
        lista_manutencoes.append(dado)

    config_ref = db.collection('configuracoes').stream()
    lista_config = []
    for doc in config_ref:
        dado = doc.to_dict()
        dado['id'] = doc.id
        lista_config.append(dado)

    veiculos_ref = db.collection('veiculos').stream()
    lista_veiculos = []
    for doc in veiculos_ref:
        dado = doc.to_dict() or {}
        dado['id'] = doc.id
        lista_veiculos.append(dado)

    return render_template(
        'pecas.html',
        manutencoes=lista_manutencoes,
        configuracoes=lista_config,
        veiculo_ativo=veiculo_ativo,
        veiculos=lista_veiculos,
    )


@app.route('/ativar_veiculo/<veiculo_id>')
@login_required
def ativar_veiculo(veiculo_id):
    try:
        doc = db.collection('veiculos').document(veiculo_id).get()
        if not doc.exists:
            flash('Veiculo nao encontrado.', 'error')
            return redirect(url_for('pecas') + '#AbaVeiculos')

        session['veiculo_ativo_id'] = veiculo_id
        flash('Veiculo ativo atualizado.', 'success')
        return redirect(url_for('index'))
    except Exception as exc:
        logging.exception('Erro ao ativar veiculo id=%s: %s', veiculo_id, exc)
        flash('Nao foi possivel trocar o veiculo ativo.', 'error')
        return redirect(url_for('pecas') + '#AbaVeiculos')


@app.route('/cadastrar_veiculo', methods=['POST'])
@login_required
def cadastrar_veiculo():
    apelido = request.form.get('apelido', '').strip()
    marca_form = request.form.get('marca', '').strip()
    modelo_form = request.form.get('modelo', '').strip()
    fipe_codigo = request.form.get('fipe_codigo', request.form.get('codigo_fipe', '')).strip()
    ano_texto = request.form.get('ano', request.form.get('ano_modelo', '')).strip()
    km_atual_texto = request.form.get('km_atual', '').strip()

    if not apelido or not marca_form or not modelo_form or not fipe_codigo or not ano_texto or not km_atual_texto:
        flash('Preencha apelido, marca, modelo, ano e KM atual da moto.', 'error')
        return redirect(url_for('veiculos'))

    try:
        ano = int(ano_texto)
    except ValueError:
        flash('Ano do modelo invalido.', 'error')
        return redirect(url_for('veiculos'))

    try:
        km_atual = _converter_int_nao_negativo(km_atual_texto)
    except ValueError:
        flash('KM atual invalido.', 'error')
        return redirect(url_for('veiculos'))

    dados_fipe = buscar_fipe(fipe_codigo, ano)
    if not dados_fipe:
        flash('Nao foi possivel encontrar o veiculo para este codigo FIPE e ano.', 'error')
        return redirect(url_for('veiculos'))

    try:
        novo_veiculo_ref = db.collection('veiculos').document()
        payload = montar_payload_veiculo(
            id_veiculo=novo_veiculo_ref.id,
            apelido=apelido,
            fipe_codigo=fipe_codigo,
            ano_modelo=ano,
            valor_fipe=dados_fipe.get('valor', ''),
            marca=marca_form or dados_fipe.get('marca', ''),
            modelo=modelo_form or dados_fipe.get('modelo', ''),
            mes_referencia=dados_fipe.get('mesReferencia', ''),
            km_atual=km_atual,
            ultimo_oleo_km=km_atual,
        )
        payload['data_cadastro'] = datetime.now().strftime('%d/%m/%Y %H:%M')
        novo_veiculo_ref.set(payload)
        for _, (nome_peca, km_limite) in LIMITE_PADRAO_PECAS.items():
            _upsert_saude_peca(novo_veiculo_ref.id, nome_peca, ultimo_km_troca=km_atual, km_limite=km_limite)
        session['veiculo_ativo_id'] = novo_veiculo_ref.id
    except Exception as exc:
        logging.exception('Erro ao cadastrar veiculo: %s', exc)
        flash('Falha ao salvar o ativo. Tente novamente.', 'error')
        return redirect(url_for('veiculos'))

    flash('Ativo cadastrado com sucesso!', 'success')
    return redirect(url_for('veiculos'))


@app.route('/editar_veiculo/<id>', methods=['GET', 'POST'])
@login_required
def editar_veiculo(id):
    doc = db.collection('veiculos').document(id).get()
    if not doc.exists:
        flash('Veiculo nao encontrado.', 'error')
        return redirect(url_for('veiculos'))

    veiculo_ativo = obter_veiculo_ativo()

    if request.method == 'POST':
        apelido = request.form.get('apelido', '').strip()
        marca_form = request.form.get('marca', '').strip()
        modelo_form = request.form.get('modelo', '').strip()
        fipe_codigo = request.form.get('fipe_codigo', request.form.get('codigo_fipe', '')).strip()
        ano_texto = request.form.get('ano', request.form.get('ano_modelo', '')).strip()
        km_atual_texto = request.form.get('km_atual', '').strip()

        if not apelido or not marca_form or not modelo_form or not fipe_codigo or not ano_texto or not km_atual_texto:
            flash('Preencha apelido, marca, modelo, ano e KM atual da moto.', 'error')
            return redirect(url_for('editar_veiculo', id=id))

        try:
            ano = int(ano_texto)
        except ValueError:
            flash('Ano do modelo invalido.', 'error')
            return redirect(url_for('editar_veiculo', id=id))

        try:
            km_atual = _converter_int_nao_negativo(km_atual_texto)
        except ValueError:
            flash('KM atual invalido.', 'error')
            return redirect(url_for('editar_veiculo', id=id))

        dados_fipe = buscar_fipe(fipe_codigo, ano)
        if not dados_fipe:
            flash('Nao foi possivel encontrar o veiculo para este codigo FIPE e ano.', 'error')
            return redirect(url_for('editar_veiculo', id=id))

        try:
            veiculo_existente = doc.to_dict() or {}
            payload = montar_payload_veiculo(
                id_veiculo=id,
                apelido=apelido,
                fipe_codigo=fipe_codigo,
                ano_modelo=ano,
                valor_fipe=dados_fipe.get('valor', ''),
                marca=marca_form or dados_fipe.get('marca', ''),
                modelo=modelo_form or dados_fipe.get('modelo', ''),
                mes_referencia=dados_fipe.get('mesReferencia', ''),
                km_atual=km_atual,
                ultimo_oleo_km=_converter_int_nao_negativo(veiculo_existente.get('ultimo_oleo_km', km_atual)),
            )
            payload['data_atualizacao'] = datetime.now().strftime('%d/%m/%Y %H:%M')
            db.collection('veiculos').document(id).update(payload)
            flash('Veiculo atualizado com sucesso!', 'success')
            return redirect(url_for('veiculos'))
        except Exception as exc:
            logging.exception('Erro ao atualizar veiculo id=%s: %s', id, exc)
            flash('Falha ao atualizar o veiculo.', 'error')
            return redirect(url_for('editar_veiculo', id=id))

    veiculo_editando = doc.to_dict() or {}
    veiculo_editando['id'] = doc.id
    lista_veiculos = listar_veiculos()
    _enriquecer_veiculos_com_metricas(lista_veiculos)

    return render_template('veiculos.html', veiculos=lista_veiculos, veiculo_ativo=veiculo_ativo, veiculo_editando=veiculo_editando)


@app.route('/atualizar_km_rapido', methods=['POST'])
@login_required
def atualizar_km_rapido():
    """Rota para atualizar KM atual do veículo via AJAX (formulário rápido do card)."""
    try:
        dados = request.get_json() or {}
        veiculo_id = str(dados.get('veiculo_id', '')).strip()
        novo_km_texto = str(dados.get('novo_km', '')).strip()

        if not veiculo_id or not novo_km_texto:
            return {'sucesso': False, 'mensagem': 'Veículo ou KM não informado.'}, 400

        novo_km = _converter_int_nao_negativo(novo_km_texto)
        if novo_km < 0:
            return {'sucesso': False, 'mensagem': 'KM inválido.'}, 400

        # Validar que o veículo pertence ao usuário (segurança)
        doc = db.collection('veiculos').document(veiculo_id).get()
        if not doc.exists:
            return {'sucesso': False, 'mensagem': 'Veículo não encontrado.'}, 404

        # Atualizar KM no veículo
        db.collection('veiculos').document(veiculo_id).update({'km_atual': novo_km})

        # Retornar sucesso com novo valor formatado
        km_formatado = _formatar_km_br(novo_km)
        return {
            'sucesso': True,
            'mensagem': f'KM atualizado para {km_formatado}',
            'novo_km': novo_km,
            'novo_km_formatado': km_formatado,
        }, 200

    except Exception as exc:
        logging.exception('Erro ao atualizar KM rápido: %s', exc)
        return {'sucesso': False, 'mensagem': 'Erro ao atualizar KM.'}, 500


@app.route('/registrar_troca_oleo', methods=['POST'])
@login_required
def registrar_troca_oleo():
    """Rota para atualizar último KM de troca de óleo."""
    try:
        dados = request.get_json() or {}
        veiculo_id = str(dados.get('veiculo_id', '')).strip()

        if not veiculo_id:
            return {'sucesso': False, 'mensagem': 'Veículo não informado.'}, 400

        # Validar que o veículo pertence ao usuário (segurança)
        doc = db.collection('veiculos').document(veiculo_id).get()
        if not doc.exists:
            return {'sucesso': False, 'mensagem': 'Veículo não encontrado.'}, 404

        veiculo_data = doc.to_dict() or {}
        km_atual = _converter_int_nao_negativo(veiculo_data.get('km_atual', 0))

        # Atualizar tabela generica de saude da peca e manter compatibilidade legada.
        _upsert_saude_peca(veiculo_id, 'Óleo', ultimo_km_troca=km_atual)
        db.collection('veiculos').document(veiculo_id).update({'ultimo_oleo_km': km_atual})

        return {
            'sucesso': True,
            'mensagem': 'Troca de óleo registrada com sucesso!',
            'novo_ultimo_oleo_km': km_atual,
            'km_rodado_oleo': 0,
            'km_desde_oleo': 0,
        }, 200

    except Exception as exc:
        logging.exception('Erro ao registrar troca de óleo: %s', exc)
        return {'sucesso': False, 'mensagem': 'Erro ao registrar troca de óleo.'}, 500


@app.route('/adicionar_oleo_100ml', methods=['POST'])
@login_required
def adicionar_oleo_100ml():
    veiculo_ativo = obter_veiculo_ativo()
    if not veiculo_ativo:
        flash('Cadastre um veículo antes de registrar reposições.', 'error')
        return redirect(url_for('veiculos'))

    try:
        km_atual = _converter_int_nao_negativo(veiculo_ativo.get('km_atual', 0))
        payload = montar_payload_manutencao(
            km=km_atual,
            servico='Reposição de Óleo 100ml',
            valor=0,
            data_padrao=datetime.now().strftime('%d/%m/%Y %H:%M'),
            obs='Adição rápida de 100ml no cárter.',
            veiculo_id=veiculo_ativo['id'],
        )
        db.collection('manutencoes').add(payload)
        flash('Reposição de 100ml registrada no histórico.', 'success')
    except Exception as exc:
        logging.exception('Erro ao registrar reposição de 100ml: %s', exc)
        flash('Não foi possível registrar a reposição de 100ml.', 'error')

    return redirect(url_for('detalhes_peca', nome_peca='Oleo'))


@app.route('/deletar_veiculo/<id>', methods=['POST'])
@login_required
def deletar_veiculo(id):
    doc = db.collection('veiculos').document(id).get()
    if not doc.exists:
        flash('Veiculo nao encontrado.', 'error')
        return redirect(url_for('veiculos'))

    try:
        for abastecimento in db.collection('abastecimentos').stream():
            dado_abastecimento = abastecimento.to_dict() or {}
            if dado_abastecimento.get('veiculo_id') == id:
                db.collection('abastecimentos').document(abastecimento.id).delete()

        for manutencao in db.collection('manutencoes').stream():
            dado_manutencao = manutencao.to_dict() or {}
            if dado_manutencao.get('veiculo_id') == id:
                db.collection('manutencoes').document(manutencao.id).delete()

        db.collection('veiculos').document(id).delete()

        if session.get('veiculo_ativo_id') == id:
            if proximo_veiculo := obter_primeiro_veiculo():
                session['veiculo_ativo_id'] = proximo_veiculo['id']
            else:
                session.pop('veiculo_ativo_id', None)

        flash('Veiculo e historico removidos com sucesso.', 'success')
        return redirect(url_for('veiculos'))
    except Exception as exc:
        logging.exception('Erro ao deletar veiculo id=%s: %s', id, exc)
        flash('Falha ao deletar veiculo.', 'error')
        return redirect(url_for('veiculos'))

@app.route('/editar/<id>', methods=['GET', 'POST'])
@login_required
def editar(id):
    veiculo_ativo = obter_veiculo_ativo()
    veiculo_ativo_id = veiculo_ativo.get('id') if veiculo_ativo else None

    if not registro_pertence_ao_veiculo('abastecimentos', id, veiculo_ativo_id):
        flash('Registro de abastecimento nao pertence ao veiculo ativo.', 'error')
        return redirect('/')

    if request.method == 'POST':
        try:
            data_padrao = converter_data_iso_para_padrao(request.form.get('data', ''))
            db.collection('abastecimentos').document(id).update({
                'km': float(request.form['km']),
                'litros': float(request.form['litros']),
                'preco_total': float(request.form['valor']),
                'data': data_padrao,
                'veiculo_id': veiculo_ativo_id,
            })
        except Exception as exc:
            logging.exception('Erro ao atualizar abastecimento id=%s via modal: %s', id, exc)
        return redirect('/')

    doc = db.collection('abastecimentos').document(id).get()
    if doc.exists:
        item = doc.to_dict(); item['id'] = doc.id; item['km'] = item.get('km', 0); item['litros'] = item.get('litros', 0); item['preco_total'] = item.get('preco_total', item.get('valor', 0))
        return render_template('editar.html', item=item)
    return redirect('/')

@app.route('/atualizar', methods=['POST'])
@login_required
def atualizar():
    id = request.form['id']
    veiculo_ativo = obter_veiculo_ativo()
    veiculo_ativo_id = veiculo_ativo.get('id') if veiculo_ativo else None

    if not registro_pertence_ao_veiculo('abastecimentos', id, veiculo_ativo_id):
        flash('Registro de abastecimento nao pertence ao veiculo ativo.', 'error')
        return redirect('/')

    try:
        payload = {
            'km': float(request.form['km']),
            'litros': float(request.form['litros']),
            'preco_total': float(request.form['valor']),
            'veiculo_id': veiculo_ativo_id,
        }
        if data_form := request.form.get('data', '').strip():
            payload['data'] = converter_data_iso_para_padrao(data_form)

        db.collection('abastecimentos').document(id).update(payload)
    except Exception as exc:
        logging.exception('Erro ao atualizar abastecimento id=%s: %s', id, exc)
    return redirect('/')

@app.route('/editar_manutencao/<id>')
@login_required
def editar_manutencao(id):
    veiculo_ativo = obter_veiculo_ativo()
    veiculo_ativo_id = veiculo_ativo.get('id') if veiculo_ativo else None

    if not registro_pertence_ao_veiculo('manutencoes', id, veiculo_ativo_id):
        flash('Registro de manutencao nao pertence ao veiculo ativo.', 'error')
        return redirect(url_for('pecas'))

    doc = db.collection('manutencoes').document(id).get()
    if doc.exists:
        item = doc.to_dict(); item['id'] = doc.id; item['km'] = item.get('km', 0); item['valor'] = item.get('valor', 0)
        return render_template('editar_manutencao.html', item=item)
    return redirect('/')

@app.route('/atualizar_manutencao', methods=['POST'])
@login_required
def atualizar_manutencao():
    id = request.form['id']
    veiculo_ativo = obter_veiculo_ativo()
    veiculo_ativo_id = veiculo_ativo.get('id') if veiculo_ativo else None

    if not registro_pertence_ao_veiculo('manutencoes', id, veiculo_ativo_id):
        flash('Registro de manutencao nao pertence ao veiculo ativo.', 'error')
        return redirect(url_for('pecas'))

    try:
        db.collection('manutencoes').document(id).update({
            'km': float(request.form['km']),
            'servico': request.form['servico'],
            'valor': float(request.form['valor']),
            'obs': request.form['obs'],
            'veiculo_id': veiculo_ativo_id,
        })
    except Exception as exc:
        logging.exception('Erro ao atualizar manutencao id=%s: %s', id, exc)
    return redirect('/')


@app.route('/webhook/telegram/<token>', methods=['POST'])
def webhook_telegram(token):
    expected_token = os.getenv('TELEGRAM_WEBHOOK_TOKEN')
    allowed_user_id = os.getenv('TELEGRAM_USER_ID')

    if not expected_token:
        logging.error('TELEGRAM_WEBHOOK_TOKEN nao configurado no ambiente.')
        return 'OK', 200

    if token != expected_token:
        logging.warning('Tentativa de webhook com token invalido.')
        return 'OK', 200

    if not allowed_user_id:
        logging.error('TELEGRAM_USER_ID nao configurado no ambiente.')
        return 'OK', 200

    update = request.get_json(silent=True) or {}
    message = update.get('message') or update.get('edited_message') or {}
    chat = message.get('chat') or {}
    chat_id = chat.get('id')
    from_user = message.get('from') or {}
    user_id = str(from_user.get('id', ''))

    if user_id != str(allowed_user_id):
        logging.warning('Mensagem ignorada de usuario nao autorizado: %s', user_id)
        return 'OK', 200

    texto = (message.get('text') or '').strip()
    if not texto:
        return 'OK', 200

    if texto.startswith('/'):
        if chat_id is not None:
            send_telegram_message(
                chat_id,
                'Ola, Braga! Mande os dados do abastecimento no formato: valor litros km (ex: 45.00 7.2 890)'
            )
        return 'OK', 200

    try:
        primeiro_veiculo = _garantir_veiculo_para_historico_legado()
        veiculo_ativo_id = primeiro_veiculo.get('id') if primeiro_veiculo else None
        if not veiculo_ativo_id:
            if chat_id is not None:
                send_telegram_message(chat_id, 'Cadastre um veiculo no painel antes de usar o Telegram.')
            return 'OK', 200

        ultimo_km_registrado = None
        docs = db.collection('abastecimentos').order_by('km', direction=firestore.Query.DESCENDING).stream()
        for doc in docs:
            dado = doc.to_dict() or {}
            if dado.get('veiculo_id') == veiculo_ativo_id:
                ultimo_km_registrado = dado.get('km', 0)
                break

        novo_abastecimento = processar_abastecimento(texto, ultimo_km_registrado, veiculo_ativo_id)
        if novo_abastecimento is None:
            if chat_id is not None:
                send_telegram_message(chat_id, '❌ Formato Invalido!\nUse: valor litros km. Ex: 50.00 8.5 120')
            return 'OK', 200

        payload = montar_payload_abastecimento(
            km=novo_abastecimento['km'],
            litros=novo_abastecimento['litros'],
            preco_total=novo_abastecimento['preco_total'],
            data_registro=novo_abastecimento['data'],
            veiculo_id=veiculo_ativo_id,
        )
        db.collection('abastecimentos').add(payload)
        _sincronizar_km_atual_veiculo(veiculo_ativo_id, payload['km'])

        if chat_id is not None:
            media_kml = novo_abastecimento.get('media_kml')
            media_txt = f'{media_kml:.1f}' if isinstance(media_kml, (int, float)) else 'N/D'
            send_telegram_message(
                chat_id,
                f'⛽ Abastecimento Registrado!\nSua media foi de {media_txt} km/L.\nO proximo milhar da Bros foi calculado automaticamente.'
            )

        return 'OK', 200
    except Exception as exc:
        logging.exception('Erro ao processar webhook do Telegram: %s', exc)
        return 'OK', 200

# NECESSÁRIO PARA O VERCEL
app = app

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=int(os.getenv('PORT', 5000)),
        debug=os.getenv('FLASK_ENV', 'development') == 'development'
    )