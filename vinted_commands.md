# Comandos da aplicação Vinted Automated Sales Control

Este ficheiro resume os comandos disponíveis na aplicação e o que cada um faz.

---

## 1. Autenticar uma conta Google

```bash
python main.py auth
```

### O que faz
- Abre o login Google OAuth.
- Obtém o email autenticado.
- Cria ou atualiza o documento em `accounts`.
- Guarda o token local da conta.

---

## 2. Correr o pipeline base para uma conta

```bash
python main.py run --account EMAIL
```

### Exemplo

```bash
python main.py run --account beaver3dcrafts@gmail.com
```

### O que faz
- Lê emails de sales.
- Lê emails de labels.
- Faz matching.
- Enriquece PDFs.
- Cria `printJobs` pendentes.

### Não faz automaticamente
- impressão
- order events
- expenses
- payouts

---

## 3. Correr o pipeline base para todas as contas ativas

```bash
python main.py run --all-accounts
```

### O que faz
- Executa o worker para todas as contas com `status = ACTIVE` na coleção `accounts`.

---

## 4. Correr pipeline base + impressão

### Para uma conta

```bash
python main.py run --account EMAIL --with-print
```

### Exemplo

```bash
python main.py run --account beaver3dcrafts@gmail.com --with-print
```

### Para todas as contas

```bash
python main.py run --all-accounts --with-print
```

### O que faz
- Executa o pipeline base.
- No fim processa `printJobs` pendentes.

---

## 5. Correr pipeline base + order events

### Para uma conta

```bash
python main.py run --account EMAIL --with-order-events
```

### Para todas as contas

```bash
python main.py run --all-accounts --with-order-events
```

### O que faz
- Executa o pipeline base.
- Processa emails de:
  - `ORDER_SHIPPED`
  - `ORDER_COMPLETED`

---

## 6. Correr pipeline base + expenses

### Para uma conta

```bash
python main.py run --account EMAIL --with-expenses
```

### Para todas as contas

```bash
python main.py run --all-accounts --with-expenses
```

### O que faz
- Executa o pipeline base.
- Processa emails de faturas para a coleção `expenses`.

---

## 7. Correr pipeline base + payouts

### Para uma conta

```bash
python main.py run --account EMAIL --with-payouts
```

### Para todas as contas

```bash
python main.py run --all-accounts --with-payouts
```

### O que faz
- Executa o pipeline base.
- Processa emails de pagamentos enviados para o banco na coleção `payouts`.

---

## 8. Correr pipeline base com várias flags ao mesmo tempo

### Exemplo completo para uma conta

```bash
python main.py run --account beaver3dcrafts@gmail.com --with-order-events --with-expenses --with-payouts --with-print
```

### Exemplo completo para todas as contas

```bash
python main.py run --all-accounts --with-order-events --with-expenses --with-payouts --with-print
```

### O que faz
- sales
- labels
- matching
- enriched PDFs
- printJobs
- order events
- expenses
- payouts
- impressão

---

## 9. Processar apenas order events

```bash
python main.py process-order-events --account EMAIL
```

### Exemplo

```bash
python main.py process-order-events --account beaver3dcrafts@gmail.com
```

### O que faz
- Lê emails de atualização de encomenda.
- Atualiza sales para `SHIPPED` ou `COMPLETED`.
- Cria `saleItems` quando necessário.

---

## 10. Processar apenas expenses

```bash
python main.py process-expenses --account EMAIL
```

### Exemplo

```bash
python main.py process-expenses --account beaver3dcrafts@gmail.com
```

### O que faz
- Lê emails de faturas.
- Cria ou atualiza documentos em `expenses`.

---

## 11. Processar apenas payouts

```bash
python main.py process-payouts --account EMAIL
```

### Exemplo

```bash
python main.py process-payouts --account beaver3dcrafts@gmail.com
```

### O que faz
- Lê emails de pagamento enviado para o banco.
- Cria ou atualiza documentos em `payouts`.

---

## 12. Full run de uma conta

```bash
python main.py full-run --account EMAIL
```

### Exemplo

```bash
python main.py full-run --account beaver3dcrafts@gmail.com
```

### O que faz
Executa tudo numa só passagem:
- worker base
- order events
- expenses
- payouts
- print

---

## 13. Full run de todas as contas

```bash
python main.py full-run --all-accounts
```

### O que faz
- Executa o fluxo completo para todas as contas ativas.

---

## 14. Reset derivado da conta (dry-run)

```bash
python reset_account_data.py --account EMAIL --derived
```

### Exemplo

```bash
python reset_account_data.py --account beaver3dcrafts@gmail.com --derived
```

### O que faz
Mostra o que seria limpo sem apagar nada:
- `saleItems`
- `printJobs`
- `events`
- reset de estados de `sales`
- reset de estados de `labels`
- limpeza de PDFs enriquecidos e impressos

---

## 15. Executar reset derivado da conta

```bash
python reset_account_data.py --account EMAIL --derived --execute
```

### Exemplo

```bash
python reset_account_data.py --account beaver3dcrafts@gmail.com --derived --execute
```

### O que faz
Executa de facto o reset derivado.

---

## 16. Reset total da conta (dry-run)

```bash
python reset_account_data.py --account EMAIL --full
```

### O que faz
Mostra o que seria apagado sem executar.

Inclui:
- `sales`
- `labels`
- `saleItems`
- `printJobs`
- `events`
- `expenses`
- `payouts`
- pasta local da conta

---

## 17. Executar reset total da conta

```bash
python reset_account_data.py --account EMAIL --full --execute
```

### Exemplo

```bash
python reset_account_data.py --account beaver3dcrafts@gmail.com --full --execute
```

### O que faz
- Apaga todos os dados da conta.
- Apaga a pasta local da conta.

---

## 18. Executar reset total e apagar também `accounts/{accountId}`

```bash
python reset_account_data.py --account EMAIL --full --delete-account-doc --execute
```

### Exemplo

```bash
python reset_account_data.py --account beaver3dcrafts@gmail.com --full --delete-account-doc --execute
```

### O que faz
- Apaga tudo da conta.
- Apaga também o documento da coleção `accounts`.

---

## Fluxos recomendados

## Fluxo normal completo

```bash
python main.py full-run --account beaver3dcrafts@gmail.com
```

## Fluxo manual faseado

```bash
python main.py run --account beaver3dcrafts@gmail.com
python main.py process-order-events --account beaver3dcrafts@gmail.com
python main.py process-expenses --account beaver3dcrafts@gmail.com
python main.py process-payouts --account beaver3dcrafts@gmail.com
```

## Reset derivado antes de voltar a testar

```bash
python reset_account_data.py --account beaver3dcrafts@gmail.com --derived --execute
```

---

## Notas importantes

- `run.py` está obsoleto e não deve ser usado.
- Usa `main.py` como ponto de entrada principal.
- Antes de testes repetidos, faz `reset-derived` para não misturares estado antigo com novo.
- Para validar expenses e payouts, usa primeiro os comandos dedicados antes de os meter no `full-run`.
