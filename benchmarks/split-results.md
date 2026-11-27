| Threads | Sensores | Threads do SO | Memória (MB) | CPU (%) | Recebidas/s (esperado) | TCP média / p95 (ms) | UDP média / p95 (ms) | UDP perdidas | Entregas TCP: ocupada % / atraso méd. ms | Entregas UDP: ocupada % / atraso méd. ms | `udp` ocupada (%) | Espera dos locks (%) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SO | 5000 | 5015 | 1129 | 30 | 70444 (100000) | 29.7 / 72.8 | 27.0 / 62.8 | 1024 | 42 / 14.9 | 31 / 6.2 | 27 | n/d |
| SO | 5000 | 5015 | 1301 | 26 | 85108 (100000) | 75.7 / 248.1 | 32.5 / 94.9 | 108043 | 59 / 280.2 | 28 / 16.4 | 24 | n/d |
| SO | 5000 | 4802 | 1063 | 28 | 76883 (100000) | 26.5 / 62.3 | 27.0 / 67.3 | 152265 | 51 / 217.9 | 38 / 12.3 | 26 | n/d |
| virtual | 5000 | 31 | 607 | 32 | 85634 (100000) | 45.4 / 151.3 | 41.1 / 150.0 | 285 | 31 / 9.7 | 38 / 10.7 | 30 | n/d |
| virtual | 5000 | 31 | 549 | 33 | 86688 (100000) | 30.6 / 67.2 | 27.4 / 56.8 | 580 | 32 / 5.7 | 40 / 6.9 | 30 | n/d |
| virtual | 5000 | 31 | 532 | 35 | 87874 (100000) | 32.5 / 80.3 | 29.8 / 77.9 | 0 | 32 / 7.2 | 39 / 8.4 | 31 | n/d |

Espera dos locks: não registrada nesta execução (erro no formato da linha, corrigido depois).
