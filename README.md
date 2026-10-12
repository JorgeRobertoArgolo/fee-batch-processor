# 🏦 Fee Batch Processor

> Sistema de alta performance para processamento em lote de tarifas bancárias com particionamento em 32 threads via Spring Batch, coreografia de eventos com Apache Kafka, PostgreSQL e Arquitetura Hexagonal.

[![Java](https://img.shields.io/badge/Java-21-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-4.x-6DB33F?style=for-the-badge&logo=spring-boot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Spring Batch](https://img.shields.io/badge/Spring_Batch-6.x-6DB33F?style=for-the-badge&logo=spring&logoColor=white)](https://spring.io/projects/spring-batch)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.4-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

---

## 🏦 Contexto de Negócio Bancário

Em instituições financeiras e bancos digitais ou tradicionais, diariamente milhões de transações geram encargos tarifários (manutenção de conta corrente, juros de cheque especial, tarifas de cartão e estornos). Essas cobranças são consolidadas em arquivos de remessa em lote (*batch files*, como arquivos CSV) e processadas no fechamento noturno (*end-of-day batch processing*).

### Desafios Arquiteturais e Regras de Negócio:
1. **Volumetria Alta e Paralelismo**: Arquivos com centenas de milhares de registros processados em janelas curtas de tempo. Resolvido com **Spring Batch particionado em 32 threads concorrentes**.
2. **Validação Cadastral Obrigatória**: Regras de conformidade bancária (BACEN) proíbem debitar contas canceladas ou bloqueadas judicialmente.
3. **Coreografia Assíncrona e Resiliente (Padrão Saga)**:
   * O lote lê as linhas do CSV e grava os registros pendentes no PostgreSQL na tabela `fee_transactions`.
   * Publica uma requisição assíncrona no Apache Kafka (`account-status-request`) com a chave de partição baseada em `accountId`.
   * O Core Bancário valida o status da conta (`ACTIVE`, `CANCELED`, `JUDICIAL_BLOCK`) e responde no tópico `account-status-response`.
   * O consumidor avalia o veredito:
     * Se `ACTIVE`: Aprova a transação (`APPROVED`), atualiza a base e publica o evento contábil no razão contábil (`accounting-ledger-entry`).
     * Se `CANCELED` ou `JUDICIAL_BLOCK`: Rejeita a transação (`REJECTED`) e encerra o fluxo sem débito.
4. **Ordenação Estrita por Conta**: O Kafka distribui as mensagens com base na `accountId` como chave de partição (*Partition Key*), assegurando ordenação estrita de eventos por cliente e paralelismo distribuído entre as 32 partições dos tópicos.

---

## 🏗️ Arquitetura Hexagonal (Ports & Adapters)

O projeto foi estruturado seguindo os princípios de isolamento de domínio da Arquitetura Hexagonal:

```
                              ┌────────────────────────────────────────┐
                              │            INBOUND ADAPTERS            │
                              │  - JobController (/jobs/process)       │
                              │  - FeeTransactionController (/fees)    │
                              │  - Spring Batch (Master / Worker 32t)  │
                              └───────────────────┬────────────────────┘
                                                  │
                                                  ▼
                              ┌────────────────────────────────────────┐
                              │            APPLICATION CORE            │
                              │  - ProcessFeeBatchUseCase (Interface)  │
                              │  - FeeTransactionServiceImpl           │
                              └───────┬────────────────────────┬───────┘
                                      │                        │
                                      ▼                        ▼
                       ┌────────────────────────┐    ┌───────────────────┐
                       │      DOMAIN CORE       │    │   MAPPER LAYER    │
                       │ - FeeTransaction       │    │ - FeeTransaction  │
                       │ - Enums / Events       │    │   Mapper          │
                       │ - FeeTransactionPort   │    └───────────────────┘
                       └──────────────┬─────────┘
                                      │
                                      ▼
                       ┌────────────────────────────────┐
                       │       OUTBOUND ADAPTERS        │
                       │  - FeeTransactionJpaAdapter    │
                       │  - FeeKafkaProducer            │
                       │  - AccountStatusResponse       │
                       │    Consumer                    │
                       │  - PostgreSQL & Apache Kafka   │
                       └────────────────────────────────┘
```

---

## 🚦 Endpoints da API

| Método | Endpoint | Descrição |
|---|---|---|
| `POST` | `/jobs/process?path=files/transactions.csv` | Dispara o job de lote particionado em background (não bloqueia a requisição HTTP) |
| `GET` | `/fees/{id}` | Consulta os detalhes, status e timestamps de auditoria de uma transação por UUID |

---

## 📡 Tópicos e Eventos Kafka

| Tópico | Chave (Partition Key) | Classe de Evento | Descrição |
|---|---|---|---|
| `account-status-request` | `accountId` | `AccountStatusRequestEvent` | Solicita a verificação cadastral da conta ao Core Bancário |
| `account-status-response` | `accountId` | `AccountStatusResponseEvent` | Retorna o veredito cadastral (`ACTIVE`, `CANCELED`, etc.) |
| `accounting-ledger-entry` | `transactionId` | `LedgerEntryEvent` | Publica o lançamento aprovado para contabilização no General Ledger |

---

## 🛠️ Tecnologias Utilizadas

* **Java 21**
* **Spring Boot 4.x & Spring Batch**
* **Spring Data JPA & Hibernate**
* **Apache Kafka** (Cluster com tópicos de 32 partições)
* **PostgreSQL 15**
* **Testcontainers** (Ambiente de testes automatizados com containers descartáveis)
* **Lombok**
* **Docker & Docker Compose**

---

## 🚀 Como Executar Localmente

### 1. Pré-requisitos
* Java 21 instalado
* Docker e Docker Compose em execução

### 2. Subir a Infraestrutura
```bash
docker compose up -d
```

### 3. Executar a Aplicação
```bash
./mvnw spring-boot:run
```

### 4. Disparar o Processamento em Lote
```bash
curl -X POST "http://localhost:8080/jobs/process?path=files/transactions.csv"
```

---

## 📄 Licença

Este projeto está sob a licença [MIT](LICENSE).
