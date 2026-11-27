# TCP vs UDP Lab: relatório detalhado (pt-BR)

Sensores simulados mandam a mesma leitura por TCP, por UDP e por um "UDP confiável" implementado aqui (UDP com ACK e retransmissão). O coletor mede, para cada protocolo, perda, ordem, duplicatas, latência e jitter, e mostra tudo num dashboard ao vivo.

Só usa a biblioteca padrão do Java (testado com Java 21). Nada para instalar.

| Arquivo | O que é |
|---|---|
| `Main.java` | Atalho: sobe o coletor e os sensores no mesmo processo e abre o dashboard |
| `Server.java` | Coletor: `ServerSocket` (uma thread por sensor), `DatagramSocket`, a rede simulada, as estatísticas e o dashboard (SSE) |
| `Sensor.java` | Sensores simulados: cada um manda a mesma leitura por TCP, UDP e UDP confiável (com ACK e retransmissão) |
| `ServerTest.java` | Testes do modelo de rede, da contagem e do agendador (sem JUnit) |
| `Benchmark.java` | Mede o coletor com 10 a 5.000 sensores: threads do SO vs virtual threads |
| `dashboard.html` | A página |

## Rodar

Na pasta do projeto (o coletor lê `dashboard.html` da pasta atual):

```bash
javac -d out *.java       # compila
java -cp out Main         # sobe coletor + 5 sensores e abre o dashboard no navegador (Ctrl-C para tudo)
```

Separado, para rodar cada parte num terminal ou os sensores em outra máquina:

```bash
java -cp out ServerTest   # testes
java -cp out Server       # coletor: TCP 9001, UDP 9002, UDP confiável 9003, dashboard em http://localhost:8765
java -cp out Sensor       # outro terminal: 5 sensores x 20 leituras/s
```

Em outra máquina: `java -cp out Sensor --host <IP do coletor>` (também aceita `--sensors`, `--rate` e `--no-rudp`). O Windows pode pedir liberação no firewall.

As mensagens são texto, uma por linha no TCP e uma por datagrama no UDP: `run;sensor;seq;ts_micros;valor`.

Se o coletor falhar com `Unable to establish loopback connection`, algo na máquina (sandbox, antivírus) está bloqueando o socket local que o servidor HTTP do Java usa internamente. Num terminal comum isso não acontece.

## Cenários e veredito

O dashboard tem 7 cenários da vida real, como LAN de supermercado, fibra de fábrica, Wi-Fi congestionado e drone via satélite. Cada um configura a rede, zera as estatísticas e explica o contexto. A seção **Quem está ganhando** julga ao vivo duas aplicações por cenário:

- **Cada leitura importa** (vendas, firmware, chat): o TCP vence se o UDP perdeu ou desordenou alguma coisa.
- **Só a leitura mais recente importa** (voz, jogo, telemetria): o UDP vence se o TCP travou 20 ms ou mais além do UDP. A comparação usa a média dos últimos 10 s, incluindo a latência máxima, porque com pouca perda os travamentos do TCP não aparecem no p95.
- Com mais de 20% de perda e o TCP passando de 500 ms no p95, ninguém vence.

Resultado medido nos 7 cenários (cerca de 12 s cada): LAN e servidor distante empatam. Nos outros quatro, o TCP vence para a aplicação em que cada leitura importa e o UDP vence para a de tempo real. No colapso, ninguém vence.

Só com jitter, sem perda, os dois quase empatam (medido: 84 vs 82 ms de média). O que trava o TCP é a retransmissão, não a variação do atraso.

## UDP confiável (terceira coluna)

Um protocolo próprio sobre UDP, na linha do que o QUIC (HTTP/3) faz: confiabilidade **sem** a ordem rígida do TCP.

- **Sensor** ([Sensor.java](../Sensor.java)): manda cada leitura para a porta 9003 e a guarda até chegar o ACK. Uma thread por sensor lê os ACKs e retransmite o que passou do tempo limite. Depois de **500 ms, desiste da leitura**, porque num dashboard ao vivo leitura velha não serve.
- **Tempo limite adaptativo** (`Sensor.Rto`), como o do TCP (RFC 6298): média e variação do tempo de ida e volta, mais três regras:
  - **regra de Karn:** não mede o tempo de leituras retransmitidas, porque o ACK pode ser de qualquer cópia;
  - **piso G = 20 ms**, que cobre o passo de ~15,6 ms do relógio do Windows;
  - **o tempo limite dobra a cada estouro** e só volta ao normal com uma medição válida.
- **Coletor** ([Server.java](../Server.java)): devolve um ACK por leitura. O ACK passa pela mesma rede simulada e também pode se perder ou atrasar. Uma cópia retransmitida que chega duas vezes é descartada e aparece em "Duplicadas".

Medido nos 7 cenários (~13 s cada, uma rodada):

| Cenário | Perdidas: UDP / UDP confiável | Latência média: UDP / UDP confiável / TCP | Veredito do UDP confiável (tempo real) |
|---|---|---|---|
| LAN cabeada | 0 / 0 | ~iguais | Igual ao UDP |
| Servidor distante | 0 / 0 | ~308 ms nos três | Igual ao UDP |
| Fibra / 4G bom | 6 / **0** | 43,9 / 47,1 / 46,1 ms | Melhor opção |
| Wi-Fi com fila | 12 / **0** | 84,6 / 93,3 / 94,6 ms | Meio-termo |
| Wi-Fi congestionado | 40 / **0** | 79,0 / 89,8 / 100,3 ms | Melhor opção |
| Rural / satélite | 153 / 151 | 356 / 357 / 446 ms | Não ajudou |
| Rede em colapso | 321 / **67** | 68 / 131 / 948 ms | Melhor opção |

**O que isso mostra:**
- **Onde a ida e volta cabe no prazo, ele recupera quase tudo** com bem menos atraso que o TCP, porque uma leitura perdida não segura as outras. No colapso, com 30% de perda, recuperou 79% das perdas com +63 ms, enquanto o TCP passou de 900 ms.
- **Com jitter alto (Wi-Fi com fila),** o tempo limite calculado fica grande, a recuperação fica lenta, e o custo médio chega perto do do TCP.
- **No cenário rural, ele não consegue ajudar:** a ida e volta (~700 ms) passa do prazo de 500 ms, e a leitura expira antes de o sensor saber que ela se perdeu. Nenhuma retransmissão resolve isso.
- **Para aplicações em que cada leitura importa,** ele não substitui o TCP: desiste de leituras velhas e entrega fora de ordem.

**Três erros de projeto que os testes revelaram (e que o TCP real já resolve):**
1. **Tempo limite sem piso:** com ida e volta estável, a variação tende a zero e o tempo limite fica colado na média. Qualquer ACK um pouco atrasado gerava retransmissão: 465 duplicatas numa LAN sem perda.
2. **Tempo limite sem backoff:** depois de trocar para uma rede mais lenta, toda leitura era retransmitida antes do ACK, a regra de Karn descartava todas as medições, e o tempo limite nunca se corrigia: 3.646 duplicatas.
3. **Retransmissões antigas contadas no período novo:** a cópia de uma leitura enviada antes do "Zerar" chegava depois dele e abria buracos falsos na sequência. Agora só contam as leituras enviadas depois do "Zerar", pelo horário que vem na própria leitura.

## Benchmark de escalabilidade

```bash
java -cp out Benchmark                  # 10 a 5.000 sensores, ~3 min; grava benchmarks/latest-results.md
java -cp out Benchmark 5000 5000 5000   # só 5.000 sensores, 3 repetições
```

O benchmark mede só TCP e UDP (os sensores rodam com `--no-rudp`), para manter a carga dos números abaixo, medidos antes de existir o UDP confiável. O coletor tem uma thread por sensor TCP. O benchmark compara dois tipos dessa thread: thread do sistema operacional (padrão) e virtual thread do Java 21 (`-Dvirtual=true`). Cada rodada usa 10 a 5.000 sensores a 20 leituras/s, numa rede limpa (0% de perda, 20 ms de atraso, sem jitter): qualquer latência acima de ~21 ms é custo do próprio coletor. Coletor e sensores rodam em JVMs separadas; os números são só do coletor.

Medido numa única rodada (i5-1235U, 10 núcleos / 12 threads, 16 GB, Windows 11), com 10 s de aquecimento e 10 s de medição:

| Threads | Sensores | Threads do SO | Memória (MB) | CPU (%) | Recebidas/s (esperado) | TCP média / p95 (ms) | UDP média / p95 (ms) | UDP perdidas |
|---|---|---|---|---|---|---|---|---|
| SO | 10 | 23 | 358 | 0 | 200 (200) | 21.4 / 22.4 | 21.5 / 22.4 | 0 |
| SO | 100 | 113 | 370 | 1 | 2001 (2000) | 21.3 / 22.4 | 21.4 / 22.5 | 0 |
| SO | 1000 | 1013 | 471 | 5 | 20157 (20000) | 21.1 / 22.4 | 21.3 / 22.8 | 0 |
| SO | 5000 | 5013 | 1197 | 26 | 59069 (100000) | 23.9 / 38.7 | 24.9 / 40.3 | 526 |
| virtual | 10 | 29 | 358 | 0 | 201 (200) | 21.5 / 22.9 | 21.4 / 22.6 | 0 |
| virtual | 100 | 29 | 361 | 3 | 2011 (2000) | 21.9 / 23.2 | 21.4 / 22.6 | 0 |
| virtual | 1000 | 29 | 365 | 14 | 20133 (20000) | 22.3 / 24.0 | 21.3 / 22.9 | 0 |
| virtual | 5000 | 29 | 486 | 27 | 68549 (100000) | 35.5 / 72.3 | 34.2 / 75.4 | 5916 |

(CPU é a fração da máquina inteira: 27% de 12 threads lógicas ≈ 3,2 núcleos ocupados.)

**O que os números mostram:**

- **Até 1.000 sensores, os dois modos dão conta.** Entregam o esperado, com a latência a ~1 ms do mínimo de 20 ms.
- **Threads do SO crescem uma por sensor; virtual threads não.** Com 5.000 sensores são 5.013 threads do sistema contra 29, e a memória comprometida cai de 1.197 para 486 MB (−59%). É o ganho que as virtual threads prometem.
- **Com 5.000 sensores, nenhum dos dois entrega as 100 mil leituras/s** (59 mil e 68 mil). A CPU não está saturada (~3 de 12 núcleos). Isso aponta para um ponto de serialização, e não para falta de processador.
- **Aparece perda real de UDP** (526 e 5.916 pacotes) com a perda simulada em 0%: o sistema descartou datagramas porque o buffer de recepção encheu antes de a aplicação ler.

**Cuidados ao ler:** é uma única rodada, num notebook com gerenciamento de energia, e o gerador de carga roda na mesma máquina. Os sensores do gerador sempre usam virtual threads, para que ele não seja o primeiro a esgotar as threads, mas a CPU dele não aparece na tabela.

### Investigação do gargalo (Lei de Amdahl)

Com threads por sensor, o que sobra de serial são as partes que só uma thread executa: a thread `udp`, que recebe os datagramas de todos os sensores; a thread que faz as entregas simuladas (`Server.Network`); e o lock das estatísticas. Pela Lei de Amdahl, essa parte serial limita o ganho, por mais threads que existam no resto. Para achar qual delas trava, o `/metrics` mede, em cada uma, a fração do tempo em que ela está trabalhando em vez de esperando, e o atraso com que cada entrega roda em relação à hora marcada.

**Antes:** uma única thread de entregas e um único lock para TCP e UDP. **Depois:** uma thread de entregas e um lock para cada protocolo, que passam a rodar em paralelo (`tcp-network` e `udp-network`).

Mediana de 3 rodadas com 5.000 sensores (dados brutos em [`benchmarks/baseline-results.md`](../benchmarks/baseline-results.md) e [`benchmarks/split-results.md`](../benchmarks/split-results.md)):

| | SO, antes | SO, depois | Virtual, antes | Virtual, depois |
|---|---|---|---|---|
| Leituras/s recebidas (esperado 100 mil) | 60 mil | 77 mil | 69 mil | **87 mil** |
| Faixa entre as 3 rodadas | 51 a 66 mil | 70 a 85 mil | 53 a 84 mil | **86 a 88 mil** |
| TCP p95 (ms) | 55 | 73 | 98 | 80 |
| UDP perdidas de verdade (em ~10 s) | 90.071 | 108.043 | 3.801 | **285** |
| Thread de entregas: ocupada / atraso médio | 68% / 305 ms | TCP 51% / 218 ms; UDP 31% / 12 ms | 50% / 15 ms | TCP 32% / 7 ms; UDP 39% / 8 ms |
| Sensores conectados (threads do SO − 13) | 4.076 a 4.551 | 4.789 a 5.002 | não medido | não medido |

**Conclusões:**

1. **A thread única de entregas era o gargalo.** Dividida em duas, a vazão subiu ~26% com virtual threads (69 → 87 mil leituras/s) e ficou estável entre as rodadas. A perda real de UDP caiu 92% (3.801 → 285), porque a thread `udp` passou a esvaziar o buffer do sistema mais rápido. O atraso das entregas caiu à metade.
2. **Com threads do SO, dividir ajuda pouco.** A vazão subiu, mas a latência e a perda de UDP continuam erráticas, com atrasos de centenas de ms. Com 5.000 threads disputando 12 processadores lógicos, as threads críticas esperam a vez de rodar, e dividir o trabalho não resolve isso. O problema é o excesso de threads, que é exatamente o que as virtual threads eliminam.
3. **O coletor não é mais o limite.** Depois da divisão, com virtual threads, nenhuma parte serial passa de 40% de ocupação e a CPU fica em ~33% da máquina. O que impede as 100 mil leituras/s provavelmente é o gerador de carga, que roda na mesma máquina. Não medi o lado do gerador, então isso é uma inferência.

**Ressalvas:** 3 rodadas é pouco, e a variação entre elas é grande perto da saturação. Na versão antiga, nem todos os sensores conseguiam conectar a tempo (4.076 a 4.551 de 5.000), o que reduz a carga e favorece o "antes". A espera pelos locks não foi registrada nas rodadas "depois" (erro no formato da linha do benchmark, corrigido depois); antes ela ficava entre 7% e 12% do tempo.

## Segurança

As portas dos sensores escutam em toda a rede (para permitir sensores em outra máquina), então tudo o que chega por elas é tratado como não confiável:
- **Linhas limitadas:** uma linha TCP com mais de 256 caracteres derruba a conexão.
- **Leituras malformadas** são descartadas.
- **Contagem limitada:** cada protocolo rastreia no máximo 20.000 sensores e 200.000 buracos abertos na sequência, então ids inventados ou saltos enormes de número de sequência não esgotam a memória.
- **Dashboard protegido:** escuta só em localhost, recusa qualquer cabeçalho `Host` diferente de `localhost`/`127.0.0.1` (o que barra DNS rebinding) e aceita apenas POST com `application/json`, o que impede outros sites de mudarem a simulação (CSRF).
- **Não há autenticação:** rode só em redes de confiança.

## Limitações (dizer na apresentação)

- A perda e o atraso são **simulados** no coletor, porque loopback não perde pacotes. No UDP, um pacote perdido é descartado. No TCP, cada perda custa um RTO de 200 ms (dobrando a cada nova perda), e nada é entregue antes do segmento anterior.
- "Zerar estatísticas" (e cada cenário) começa uma rede nova: as leituras ainda "no ar" e a fila de retransmissões do TCP são descartadas, para o cenário anterior não contaminar o próximo.
- O veredito usa limiares escolhidos para a demonstração (20 ms, 20%, 500 ms), não valores de norma.
- As entregas simuladas rodam num agendador próprio (`Server.Network`), e não no `ScheduledExecutorService`: no Windows, este acorda em passos de 15,6 ms, e medimos +8 ms de média num link de 5 ms.
- **A latência pode subir ~10 ms de forma intermitente no Windows.** Num mesmo processo, sem nenhuma mudança, a espera das entregas variou entre 0,7 ms e 10 ms de atraso, e a LAN entre 6 e 15 ms. O mais provável é a economia de energia do Windows deixar de honrar a resolução de 1 ms do relógio para processos em segundo plano. Não confirmei a causa. Antes de apresentar, rode o cenário LAN: se der ~15 ms em vez de ~6, a diferença atinge TCP e UDP igualmente, e os vereditos não mudam.
- A latência compara o relógio do sensor com o do coletor. Com sensores em outra máquina, a diferença entre os relógios entra na conta (o problema da falta de relógio global, da Aula 14). Ela também desloca o corte do "Zerar", que usa o horário de envio da leitura.
