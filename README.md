Moto Tracker - backend local rapido

Como rodar localmente no Windows (forma mais facil):

1. Abra o terminal na pasta do projeto.
2. Rode:
   .\run-backend.cmd

Alternativa via PowerShell:

1. Abra o PowerShell na pasta do projeto.
2. Rode:
   powershell -NoProfile -ExecutionPolicy Bypass -File .\run-backend.ps1

O script faz automaticamente:
- cria .venv se nao existir
- instala dependencias
- cria .env a partir do .env.example (na primeira vez)
- inicia o Flask em modo debug

Configuracao obrigatoria do Firebase:

Use no arquivo .env:

- FIREBASE_KEY com o JSON completo da service account em linha unica

Variaveis uteis no .env:

- APP_PASSWORD: senha do login web
- FLASK_SECRET_KEY: segredo de sessao do Flask
- FLASK_ENV: development ou production
- PORT: porta local (padrao 5000)

Atalho alternativo sem script:

1. python -m venv .venv
2. .\.venv\Scripts\Activate.ps1
3. pip install -r requirements.txt
4. flask run --debug

Se o Windows abrir um .txt ao inves de executar:

- Nao de duplo clique no arquivo .ps1
- Rode sempre pelo terminal com um destes comandos:
   - .\run-backend.cmd
   - powershell -NoProfile -ExecutionPolicy Bypass -File .\run-backend.ps1
