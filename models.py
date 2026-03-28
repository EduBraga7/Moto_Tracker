from dataclasses import dataclass


@dataclass
class SaudePeca:
    veiculo_id: str
    nome_peca: str
    ultimo_km_troca: int
    km_limite: int

    def to_firestore(self) -> dict:
        return {
            'veiculo_id': str(self.veiculo_id or '').strip(),
            'nome_peca': str(self.nome_peca or '').strip(),
            'ultimo_km_troca': int(self.ultimo_km_troca or 0),
            'km_limite': int(self.km_limite or 0),
        }
