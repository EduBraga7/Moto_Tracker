import json
import logging
import os
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore

from backend.models import SaudePeca
from backend.utils import converter_int_nao_negativo, normalizar_texto

logger = logging.getLogger(__name__)

DEFAULT_PART_LIMITS = {
    'oleo': ('Óleo', 1000),
    'relacao': ('Relação', 15000),
    'pneu': ('Pneus', 12000),
}


def _obter_limite_padrao_peca(nome_peca):
    nome_norm = normalizar_texto(nome_peca)
    for chave_norm, (nome_canonico, limite) in DEFAULT_PART_LIMITS.items():
        if chave_norm in nome_norm or nome_norm in chave_norm:
            return nome_canonico, int(limite)
    return str(nome_peca or 'Peça').strip() or 'Peça', 1000


def init_firestore():
    if not firebase_admin._apps:
        firebase_key = os.getenv('FIREBASE_KEY')
        if not firebase_key:
            raise RuntimeError('Variavel de ambiente FIREBASE_KEY ausente.')

        try:
            key_dict = json.loads(firebase_key)
            cred = credentials.Certificate(key_dict)
            firebase_admin.initialize_app(cred)
        except Exception as exc:
            logger.exception('Falha ao inicializar Firebase com FIREBASE_KEY: %s', exc)
            raise RuntimeError('FIREBASE_KEY invalida para inicializacao do Firebase.') from exc

    return firestore.client()


db = init_firestore()


def existe_registro_legado_sem_veiculo(nome_colecao):
    try:
        for doc in db.collection(nome_colecao).stream():
            dado = doc.to_dict() or {}
            if not dado.get('veiculo_id'):
                return True
    except Exception as exc:
        logger.exception('Erro ao verificar legados em %s: %s', nome_colecao, exc)
    return False


def criar_veiculo_padrao():
    ref = db.collection('veiculos').document()
    payload = {
        'id': ref.id,
        'apelido': 'Minha Moto',
        'fipe_codigo': 'N/A',
        'ano_modelo': 0,
        'valor_fipe': 'N/A',
        'km_atual': 0,
        'ultimo_oleo_km': 0,
        'marca': '',
        'modelo': '',
        'mes_referencia': '',
        'data_cadastro': datetime.now().strftime('%d/%m/%Y %H:%M'),
    }
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
        logger.exception('Erro ao obter primeiro veiculo: %s', exc)
    return None


def garantir_veiculo_para_historico_legado(session):
    primeiro_veiculo = obter_primeiro_veiculo()
    if primeiro_veiculo:
        return primeiro_veiculo

    existe_legado = existe_registro_legado_sem_veiculo('abastecimentos') or existe_registro_legado_sem_veiculo('manutencoes')
    if not existe_legado:
        return None

    try:
        return criar_veiculo_padrao()
    except Exception as exc:
        logger.exception('Erro ao criar veiculo padrao para migracao: %s', exc)
        return None


def obter_veiculo_ativo(session):
    veiculo_ativo_id = session.get('veiculo_ativo_id')
    if veiculo_ativo_id:
        try:
            doc = db.collection('veiculos').document(veiculo_ativo_id).get()
            if doc.exists:
                dado = doc.to_dict() or {}
                dado['id'] = doc.id
                return dado
        except Exception as exc:
            logger.exception('Erro ao obter veiculo ativo da sessao: %s', exc)

    primeiro_veiculo = garantir_veiculo_para_historico_legado(session)
    if primeiro_veiculo:
        session['veiculo_ativo_id'] = primeiro_veiculo['id']
    return primeiro_veiculo


def migrar_registros_sem_veiculo_id(nome_colecao, veiculo_id, session):
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
        logger.exception('Erro ao migrar registros legados da colecao %s: %s', nome_colecao, exc)

    session[chave_migracao] = True


def listar_veiculos():
    lista_veiculos = []
    try:
        for doc in db.collection('veiculos').stream():
            dado = doc.to_dict() or {}
            dado['id'] = doc.id
            lista_veiculos.append(dado)
    except Exception as exc:
        logger.exception('Erro ao listar veiculos: %s', exc)

    return lista_veiculos


def upsert_saude_peca(veiculo_id, nome_peca, ultimo_km_troca=None, km_limite=None):
    if not veiculo_id:
        return

    nome_canonico, limite_padrao = _obter_limite_padrao_peca(nome_peca)
    doc_id = f"{veiculo_id}__{normalizar_texto(nome_canonico)}"
    doc_ref = db.collection('saude_pecas').document(doc_id)
    doc = doc_ref.get()
    atual = doc.to_dict() or {}

    payload = {
        'veiculo_id': veiculo_id,
        'nome_peca': nome_canonico,
        'ultimo_km_troca': converter_int_nao_negativo(atual.get('ultimo_km_troca', 0)),
        'km_limite': converter_int_nao_negativo(atual.get('km_limite', limite_padrao)) or limite_padrao,
    }

    if ultimo_km_troca is not None:
        payload['ultimo_km_troca'] = converter_int_nao_negativo(ultimo_km_troca)
    if km_limite is not None:
        payload['km_limite'] = max(1, converter_int_nao_negativo(km_limite))

    saude_peca = SaudePeca(
        veiculo_id=payload['veiculo_id'],
        nome_peca=payload['nome_peca'],
        ultimo_km_troca=payload['ultimo_km_troca'],
        km_limite=payload['km_limite'],
    )
    doc_ref.set(saude_peca.to_firestore(), merge=True)


def buscar_documento_veiculo(veiculo_id):
    return db.collection('veiculos').document(str(veiculo_id)).get()


def sincronizar_km_atual_veiculo(veiculo_id, km_candidato):
    km_novo = converter_int_nao_negativo(km_candidato)
    if not veiculo_id:
        return km_novo

    try:
        doc_ref = db.collection('veiculos').document(veiculo_id)
        doc = doc_ref.get()
        if not doc.exists:
            return km_novo

        km_atual = converter_int_nao_negativo((doc.to_dict() or {}).get('km_atual', 0))
        if km_novo > km_atual:
            doc_ref.update({'km_atual': km_novo})
            return km_novo
        return km_atual
    except Exception as exc:
        logger.exception('Erro ao sincronizar km_atual do veiculo id=%s: %s', veiculo_id, exc)
        return km_novo


def buscar_abastecimentos_por_veiculo(veiculo_id):
    itens = []
    for doc in db.collection('abastecimentos').order_by('km', direction=firestore.Query.DESCENDING).stream():
        dado = doc.to_dict() or {}
        if veiculo_id and dado.get('veiculo_id') != veiculo_id:
            continue
        dado['id'] = doc.id
        itens.append(dado)
    return itens


def buscar_manutencoes_por_veiculo(veiculo_id):
    itens = []
    for doc in db.collection('manutencoes').order_by('km', direction=firestore.Query.DESCENDING).stream():
        dado = doc.to_dict() or {}
        if veiculo_id and dado.get('veiculo_id') != veiculo_id:
            continue
        dado['id'] = doc.id
        itens.append(dado)
    return itens


def buscar_saude_pecas_por_veiculo(veiculo_id):
    itens = []
    for doc in db.collection('saude_pecas').where('veiculo_id', '==', veiculo_id).stream():
        dado = doc.to_dict() or {}
        dado['id'] = doc.id
        itens.append(dado)
    return itens


def buscar_todos_registros(nome_colecao):
    return [doc.to_dict() or {} for doc in db.collection(nome_colecao).stream()]
