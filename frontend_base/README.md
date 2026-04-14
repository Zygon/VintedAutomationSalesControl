# Vinted Ops Dashboard - Base Streamlit

## O que isto faz
- Dashboard local multi-página
- Filtros globais por conta e período
- Tabelas para vendas, shipments, despesas e payouts
- CRUD básico de produtos
- Helper para gerar o próximo SKU

## Pré-requisitos
- Python 3.11+
- Firestore acessível com service account
- Variável de ambiente:
  - `FIREBASE_SERVICE_ACCOUNT_FILE`
  - ou `GOOGLE_APPLICATION_CREDENTIALS`

## Instalação
```bash
pip install -r requirements.txt
```

## Arranque
```bash
streamlit run app.py
```

## Notas
- O frontend é intencionalmente simples. Primeiro validar dados, depois embelezar.
- `SKU Helper` só funciona com uma conta específica selecionada.
- As coleções novas esperadas são:
  - `products`
  - `skuCounters`
  - `generatedSkus`
