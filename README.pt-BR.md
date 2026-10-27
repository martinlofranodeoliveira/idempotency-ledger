# idempotency-ledger

Uma biblioteca Python pequena para coordenar novas tentativas usando um registro
local em SQLite. Um chamador recebe uma posse temporária, conclusões com tokens
antigos são recusadas, e o resultado JSON concluído pode ser recuperado para
sempre. Requer Python 3.10+ e não tem dependências de execução.

[English documentation](README.md)

É útil para um worker local de baixo volume, importações em uma única máquina e
scripts que recebem a mesma solicitação várias vezes. A aplicação executa o
trabalho e define como calcular a impressão digital da solicitação.

## Instalar e experimentar

Dentro do repositório:

```sh
python -m pip install .
python -B -m unittest discover -s tests -v
PYTHONPATH=. python -B examples/synthetic_demo.py
PYTHONPATH=. python -B examples/synthetic_long_job.py
```

A demonstração usa apenas dados sintéticos e remove seu banco temporário ao
terminar. A instalação usa `setuptools`; a execução e os testes usam somente a
biblioteca padrão do Python.

## Exemplo da API

```python
import hashlib
from pathlib import Path
from idempotency_ledger import Ledger

# Criar uma vez, em um caminho exclusivo que ainda não exista.
ledger = Ledger.create(Path("/tmp/meu-registro-exclusivo.sqlite3"))
# Nas próximas execuções, usar Ledger(path) para abrir o registro existente.

fingerprint = hashlib.sha256(b'{"fatura":42,"versao":1}').hexdigest()
claim = ledger.claim("fatura:42", fingerprint, now_ms=1000, lease_ms=5000)
if claim["status"] == "claimed":
    resultado = {"fatura": 42, "estado": "preparada"}  # Dados sintéticos.
    aceito = ledger.complete("fatura:42", claim["token"], resultado, now_ms=1001)
    print("Resultado aceito pelo registro:", aceito)
elif claim["status"] == "replay":
    print(claim["result"])
elif claim["status"] == "busy":
    print("Posse atual válida até:", claim["expires_at"])
elif claim["status"] == "conflict":
    print("Esta chave já representa outra solicitação.")
```

A biblioteca nunca consulta o relógio. A aplicação fornece `now_ms`, em uma base
de tempo consistente entre os chamadores. Os tempos do exemplo são sintéticos.
`expires_at` usa a mesma base. O token é uma identificação opaca de posse, sem
ordenação crescente para sistemas externos.

## Contrato

`Ledger.create(path) -> Ledger` cria um banco identificado, com permissões `0600`,
em um arquivo temporário no mesmo diretório. Publica o arquivo com um hard link
atômico, sem substituir nenhuma entrada existente, incluindo links simbólicos
quebrados. O diretório pai precisa existir e o sistema de arquivos precisa aceitar
hard links. O arquivo temporário é removido após sucesso ou falhas comuns. A
publicação é atômica; a sobrevivência da nova entrada após qualquer falta de
energia não é garantida.

`Ledger(path)` abre apenas um registro existente com formato reconhecido. Caminho
inexistente, outro banco SQLite, versão desconhecida ou esquema alterado causam
`LedgerError`. A identificação inicial é somente leitura e não inicializa nem
recupera outro banco SQLite. Cada operação verifica a identificação novamente;
isso identifica o formato e não autentica arquivos contra substituição hostil.

`claim(key, fingerprint, *, now_ms, lease_ms) -> dict` retorna exatamente:

| Situação | Retorno |
| --- | --- |
| Chave nova | `{"status": "claimed", "token": "<64 caracteres hexadecimais minúsculos>", "expires_at": now_ms + lease_ms}` |
| Impressão digital diferente, mesmo após expirar ou concluir | `{"status": "conflict"}` |
| Mesma impressão digital e resultado concluído | `{"status": "replay", "result": <valor JSON decodificado>}` |
| Mesma impressão digital e posse pendente válida | `{"status": "busy", "expires_at": <prazo existente>}` |
| Mesma impressão digital e posse pendente expirada | Novo retorno `claimed`, com novo token e novo prazo |

A posse vale enquanto `now_ms < expires_at`; no instante exato do vencimento,
pode ser obtida novamente. A impressão digital original é preservada. Resultados
concluídos nunca expiram e cada recuperação produz um objeto independente.

`complete(key, token, result, *, now_ms) -> bool` armazena o resultado somente se
a chave estiver pendente, o token for o atual e `now_ms < expires_at`. Retorna
`False` para chave ausente, token antigo, prazo expirado ou conclusão anterior.
O resultado é validado mesmo para uma chave ausente ou um token antigo. O retorno
indica apenas se o registro aceitou a conclusão.

`renew(key, token, *, now_ms, lease_ms) -> bool` renova explicitamente uma posse
pendente somente se o token for o atual e `now_ms < expires_at`. O novo prazo é
`max(prazo_anterior, now_ms + lease_ms)`: uma solicitação de prazo menor ou igual
retorna `True` sem encurtar a posse. Token, impressão digital, estado e resultado
são preservados. Retorna `False` para chave ausente, token antigo, posse expirada
ou concluída. No instante exato do vencimento, a renovação não pode reativar a
posse, inclusive após sua aquisição por outro token. A validação de chave, token,
tempo, prazo e estouro da soma ocorre antes de qualquer transação.

Trabalhos longos podem chamar `renew` de forma cooperativa entre etapas de duração
limitada, antes de vencer o prazo atual. Se retornar `False`, pare de iniciar
trabalho sob essa posse e descarte seu resultado. O [exemplo sintético de trabalho
longo](examples/synthetic_long_job.py) usa chamadas explícitas e tempos fornecidos
pela aplicação. Cada prazo solicitado tem limite de um dia, mas renovações
sucessivas permitem uma duração total maior que um dia. Não há renovação
automática, agendador ou consulta ao relógio em segundo plano.

Entradas inválidas e falhas do banco causam `LedgerError`, com mensagens sem
caminhos, SQL ou conteúdo das solicitações. Uma entrada inválida não altera o
registro.

## Limites de entrada

- `key`: `str` exata, ASCII `[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}`.
- `fingerprint` e `token`: `str` exata com 64 caracteres hexadecimais minúsculos.
- `now_ms`: `int` exato, sem aceitar `bool`, entre zero e `2**63-1`.
- `lease_ms`: `int` exato, sem aceitar `bool`, de 1 a 86.400.000; a soma com
  `now_ms` não pode ultrapassar `2**63-1`.
- `result`: somente tipos exatos `None`, `bool`, `int`, `float` finito, `str`,
  `list` e `dict` com chaves do tipo `str` exato.

Tuplas, subclasses, números não finitos, pontos de código substitutos Unicode e ciclos
são rejeitados. Estruturas compartilhadas sem ciclos são permitidas. O limite é
de 32 contêineres aninhados, contando a raiz, e 65.536 bytes de JSON compacto em
UTF-8, incluindo escapes e pontuação. Inteiros grandes dentro desse limite não
alteram a configuração global de conversão decimal do Python.

## Concorrência e limites operacionais

Instâncias, threads e processos independentes coordenam o registro por transações
SQLite `BEGIN IMMEDIATE`. Cada operação fecha suas conexões. A espera por cada
aquisição de bloqueio dura aproximadamente dois segundos antes de `LedgerError`;
a aplicação define sua política de novas tentativas.

Use um banco exclusivo em um **sistema de arquivos local e confiável**, para
volume modesto. SQLite serializa as escritas. Não é um serviço de bloqueio
distribuído, não é indicado para sistemas de arquivos de rede e não inclui
integração com uma frota de produção. Não altere as tabelas nem o modo de journal
externamente. Não há servidor, rede, execução automática, renovação automática de posse ou
política de retenção. Resultados permanecem no banco e a aplicação deve planejar
capacidade, ciclo de vida e proteção dos dados.

**Não há garantia de executar um efeito externo exatamente uma vez.** Uma posse
expirada não interrompe o worker antigo. Ele pode executar um efeito e falhar
antes de registrar o resultado; uma nova tentativa pode repetir esse efeito. O
token protege somente a conclusão neste registro. Serviços externos e transações
de negócio precisam de sua própria idempotência ou mecanismo de proteção. Os
tempos fornecidos pelo chamador são confiados; informar um tempo passado pode
violar a validade real do prazo.
A renovação não interrompe um worker expirado, não protege efeitos em sistemas
externos e não acrescenta garantia de execução exatamente uma vez.

Os testes públicos usam apenas bancos sintéticos temporários. Cobrem disputas
entre threads e processos, conclusões concorrentes, tokens antigos, conflitos,
repetição permanente, validação JSON, limites de tempo/tamanho/profundidade,
contenção limitada, modo privado, criação sem substituição, limpeza de conexões,
falhas de criação e preservação de bancos alheios. Os testes também cobrem
renovação disputando com nova aquisição e conclusão. O CI testa Python 3.10,
3.12 e 3.14, com permissões de leitura e sem persistir credenciais do checkout.

MIT © 2026 Martin Lofrano.
