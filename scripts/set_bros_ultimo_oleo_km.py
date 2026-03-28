from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import _normalizar_texto, db

TARGET_KM = 64913


def main() -> None:
    updated = 0
    inspected = 0

    for doc in db.collection("veiculos").stream():
        inspected += 1
        data = doc.to_dict() or {}

        apelido = data.get("apelido", "")
        modelo = data.get("modelo", "")
        texto = _normalizar_texto(f"{apelido} {modelo}")

        if "bros" not in texto:
            continue

        db.collection("veiculos").document(doc.id).update({"ultimo_oleo_km": TARGET_KM})
        updated += 1
        print(f"updated veiculo={doc.id} ultimo_oleo_km={TARGET_KM}")

    print(f"inspected={inspected} updated={updated}")


if __name__ == "__main__":
    main()
