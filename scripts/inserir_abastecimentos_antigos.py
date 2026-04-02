import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime

# Inicialização do Firebase (ajuste o caminho do seu arquivo de credenciais)
cred = credentials.Certificate("firebase-key.json")  # Coloque o nome do seu arquivo de chave na raiz do projeto
firebase_admin.initialize_app(cred)
db = firestore.client()

# Busca automática do único veículo cadastrado
veiculos = list(db.collection('veiculos').stream())
if not veiculos:
    raise Exception('Nenhum veículo cadastrado no Firestore!')
veiculo_id = veiculos[0].id
print(f'Veículo encontrado: {veiculo_id}')

abastecimentos = [
    {"data": "2026-01-17", "km_parcial": 382, "litros": 6.682, "valor_total": 43.36},
    {"data": "2026-02-13", "km_parcial": 597, "litros": 6.525, "valor_total": 44.30},
    {"data": "2026-03-01", "km_parcial": 835, "litros": 6.970, "valor_total": 47.33},
    {"data": "2026-03-15", "km_parcial": 1055, "litros": 7.157, "valor_total": 50.01},
]

odometro_total = 0
for ab in abastecimentos:
    odometro_total += ab["km_parcial"]
    registro = {
        "veiculo_id": veiculo_id,
        "data": ab["data"],
        "data_iso": datetime.strptime(ab["data"], "%Y-%m-%d").isoformat(),
        "km": odometro_total,
        "km_parcial": ab["km_parcial"],
        "litros": ab["litros"],
        "valor_total": ab["valor_total"],
    }
    db.collection("abastecimentos").add(registro)
    print(f"Abastecimento em {ab['data']} inserido com odômetro {odometro_total} km.")

print("Inserção concluída.")
