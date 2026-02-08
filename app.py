import functools
from flask import Flask, render_template, request, redirect, url_for, session, flash
from datetime import datetime

# --- IMPORTAÇÕES DO FIREBASE ---
import firebase_admin
from firebase_admin import credentials, firestore

# --- CONFIGURAÇÃO INICIAL ---
app = Flask(__name__)
app.secret_key = 'chave_super_secreta_moto_tracker' 
SENHA_SECRETA = "admin"  # <--- SUA SENHA

# --- CONEXÃO COM O FIREBASE ---
try:
    if not firebase_admin._apps: # Evita erro de inicializar 2 vezes
        cred = credentials.Certificate("serviceAccountKey.json")
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("✅ Conectado ao Firebase com sucesso!")
except Exception as e:
    print(f"❌ Erro ao conectar no Firebase: {e}")

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

# ================= ROTA PRINCIPAL (CORRIGIDA) =================
@app.route('/')
@login_required 
def index():
    # --- 1. BUSCAR DADOS ---
    abastecimentos_ref = db.collection('abastecimentos').order_by('km', direction=firestore.Query.DESCENDING).stream()
    lista_abastecimentos = []
    
    for doc in abastecimentos_ref:
        dado = doc.to_dict()
        dado['id'] = doc.id
        # CORREÇÃO DE ERRO: Garante que os campos existem
        dado['km'] = float(dado.get('km', 0))
        dado['litros'] = float(dado.get('litros', 0))
        # Tenta 'preco_total', se não achar tenta 'valor', senão 0
        dado['preco_total'] = float(dado.get('preco_total', dado.get('valor', 0)))
        lista_abastecimentos.append(dado)

    manutencoes_ref = db.collection('manutencoes').order_by('km', direction=firestore.Query.DESCENDING).stream()
    lista_manutencoes = []
    for doc in manutencoes_ref:
        dado = doc.to_dict()
        dado['id'] = doc.id
        dado['valor'] = float(dado.get('valor', 0)) # Proteção
        lista_manutencoes.append(dado)

    config_ref = db.collection('configuracoes').stream()
    lista_config = []
    for doc in config_ref:
        dado = doc.to_dict()
        dado['id'] = doc.id
        lista_config.append(dado)

    # --- 2. CÁLCULOS KPI ---
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

        # Gráfico Linha
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

    # Gráfico Barras
    gastos_mensais = {}
    
    for a in lista_abastecimentos:
        data = a.get('data', '01/01/2000') # Data padrão se falhar
        mes_ano = data[3:10] 
        gastos_mensais[mes_ano] = gastos_mensais.get(mes_ano, 0) + a['preco_total']
        
    for m in lista_manutencoes:
        data = m.get('data', '01/01/2000')
        mes_ano = data[3:10]
        gastos_mensais[mes_ano] = gastos_mensais.get(mes_ano, 0) + m['valor']

    for mes in sorted(gastos_mensais.keys()): 
        labels_barra.append(mes)
        values_barra.append(round(gastos_mensais[mes], 2))

    # --- 3. SAÚDE DAS PEÇAS ---
    saude_pecas = []
    km_atual_moto = lista_abastecimentos[0]['km'] if lista_abastecimentos else 0
    
    for config in lista_config:
        nome_busca = config.get('nome', '').strip().lower()
        km_vida_util = float(config.get('km_vida_util', 1000))
        
        km_ultima_troca = 0
        for m in lista_manutencoes:
            if nome_busca in m.get('servico', '').lower():
                km_ultima_troca = float(m.get('km', 0))
                break 

        km_rodado = km_atual_moto - km_ultima_troca
        km_restante = km_vida_util - km_rodado
        porcentagem = (km_restante / km_vida_util) * 100
        
        saude_pecas.append({
            'nome': config.get('nome', 'Peça'),
            'pct': porcentagem,
            'km_restante': km_restante,
            'km_rodado': km_rodado
        })

    # --- 4. TABELA FINAL ---
    abastecimentos_exibicao = []
    for i in range(len(lista_abastecimentos)):
        item = lista_abastecimentos[i].copy()
        item['kml'] = "---"
        
        if item['litros'] > 0:
            item['preco_litro'] = round(item['preco_total'] / item['litros'], 2)
        else:
            item['preco_litro'] = 0

        if i < len(lista_abastecimentos) - 1:
            anterior = lista_abastecimentos[i+1]
            diff = item['km'] - anterior['km']
            if diff > 0 and item['litros'] > 0:
                calc = diff / item['litros']
                item['kml'] = f"{calc:.1f}"
        
        abastecimentos_exibicao.append(item)

    return render_template('index.html', 
                           abastecimentos=abastecimentos_exibicao,
                           manutencoes=lista_manutencoes,
                           configuracoes=lista_config,
                           saude_pecas=saude_pecas,
                           kpi_media=f"{media_geral:.1f}",
                           kpi_custo_km=f"{custo_km:.2f}",
                           kpi_total=f"{total_geral:.2f}",
                           labels_linha=labels_linha,
                           values_linha=values_linha,
                           labels_barra=labels_barra,
                           values_barra=values_barra)

# ================= ROTAS DE AÇÃO =================

@app.route('/adicionar_rapido', methods=['POST'])
@login_required
def adicionar_rapido():
    texto = request.form['smart_text']
    partes = texto.split()
    
    if len(partes) == 3:
        try:
            valor_total = float(partes[0])
            litros = float(partes[1])
            km_parcial = int(partes[2]) 
            
            docs = db.collection('abastecimentos').order_by('km', direction=firestore.Query.DESCENDING).limit(1).get()
            
            km_final_real = 0
            if len(docs) > 0:
                ultimo_dado = docs[0].to_dict()
                km_antigo_total = int(ultimo_dado.get('km', 0))
                km_antigo_final = km_antigo_total % 1000 
                
                if km_parcial < km_antigo_final:
                    km_base = (km_antigo_total // 1000) * 1000 + 1000
                    km_final_real = km_base + km_parcial
                else:
                    km_base = (km_antigo_total // 1000) * 1000
                    km_final_real = km_base + km_parcial
            else:
                km_final_real = km_parcial 

            data_atual = datetime.now().strftime('%d/%m/%Y %H:%M')
            
            db.collection('abastecimentos').add({
                'km': km_final_real,
                'litros': litros,
                'preco_total': valor_total, # Garante que salva com esse nome
                'data': data_atual
            })
            
        except Exception as e:
            print(f"Erro: {e}")
            
    return redirect('/')

@app.route('/adicionar_manutencao', methods=['POST'])
@login_required
def adicionar_manutencao():
    try:
        db.collection('manutencoes').add({
            'km': float(request.form['km']),
            'servico': request.form['servico'],
            'valor': float(request.form['valor']),
            'obs': request.form['obs'],
            'data': datetime.now().strftime('%d/%m/%Y %H:%M')
        })
    except Exception as e:
        print(f"Erro: {e}")
    return redirect('/')

@app.route('/salvar_config', methods=['POST'])
@login_required
def salvar_config():
    try:
        db.collection('configuracoes').add({
            'nome': request.form['nome'],
            'km_vida_util': float(request.form['km'])
        })
    except:
        pass
    return redirect('/')

# ================= ROTAS DE DELETAR/EDITAR =================

@app.route('/deletar/<id>')
@login_required
def deletar(id):
    db.collection('abastecimentos').document(id).delete()
    return redirect('/')

@app.route('/deletar_manutencao/<id>')
@login_required
def deletar_manutencao(id):
    db.collection('manutencoes').document(id).delete()
    return redirect('/')

@app.route('/deletar_config/<id>')
@login_required
def deletar_config(id):
    db.collection('configuracoes').document(id).delete()
    return redirect('/')

@app.route('/editar/<id>')
@login_required
def editar(id):
    doc = db.collection('abastecimentos').document(id).get()
    if doc.exists:
        item = doc.to_dict()
        item['id'] = doc.id
        # Garante que os campos existam para o HTML não quebrar
        item['km'] = item.get('km', 0)
        item['litros'] = item.get('litros', 0)
        item['preco_total'] = item.get('preco_total', item.get('valor', 0))
        return render_template('editar.html', item=item)
    return redirect('/')

@app.route('/atualizar', methods=['POST'])
@login_required
def atualizar():
    id = request.form['id']
    try:
        db.collection('abastecimentos').document(id).update({
            'km': float(request.form['km']),
            'litros': float(request.form['litros']),
            'preco_total': float(request.form['valor'])
        })
    except:
        pass
    return redirect('/')

@app.route('/editar_manutencao/<id>')
@login_required
def editar_manutencao(id):
    doc = db.collection('manutencoes').document(id).get()
    if doc.exists:
        item = doc.to_dict()
        item['id'] = doc.id
        item['km'] = item.get('km', 0)
        item['valor'] = item.get('valor', 0)
        return render_template('editar_manutencao.html', item=item)
    return redirect('/')

@app.route('/atualizar_manutencao', methods=['POST'])
@login_required
def atualizar_manutencao():
    id = request.form['id']
    try:
        db.collection('manutencoes').document(id).update({
            'km': float(request.form['km']),
            'servico': request.form['servico'],
            'valor': float(request.form['valor']),
            'obs': request.form['obs']
        })
    except:
        pass
    return redirect('/')

app = app
#if __name__ == '__main__':
    #app.run(debug=True, host='0.0.0.0', port=5000)
    
