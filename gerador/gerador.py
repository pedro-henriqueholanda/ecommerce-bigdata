#!/usr/bin/env python3
"""
Gerador de eventos de um e-commerce: cliques, carrinho, compras e status de entrega.

Cada evento vira UMA linha JSON num arquivo de log. O Flume (source TAILDIR) acompanha
esse arquivo e replica os eventos para o Kafka (stream -> Flink) e para o HDFS (logs brutos).

- Volume/Velocidade: taxa configurável (--eps) com picos aleatórios ("flash sale").
- Eventos fora de ordem: uma fração (--fora-de-ordem) sai com event_time atrasado de 2 a 30 s,
  simulando app offline / rede lenta. Isso exercita os watermarks do Flink.
- Trend topics: a cada ~2 min um produto "viraliza" e recebe muito mais visualizações.

Uso:
    python3 gerador.py --saida /data/logs/events.log --eps 20
"""
import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

CATEGORIAS = {
    "eletronicos": (150.0, 4000.0),
    "moda": (40.0, 400.0),
    "casa": (30.0, 900.0),
    "livros": (20.0, 150.0),
    "esportes": (50.0, 1200.0),
    "beleza": (15.0, 300.0),
}
CIDADES = ["Fortaleza", "São Paulo", "Recife", "Salvador",
           "Belo Horizonte", "Manaus", "Porto Alegre", "Curitiba"]
FLUXO_ENTREGA = ["aprovado", "em_separacao", "enviado",
                 "em_transito", "saiu_para_entrega", "entregue"]


def criar_catalogo(n=80):
    rng = random.Random(42)  # catálogo fixo entre execuções
    cats = list(CATEGORIAS)
    produtos = []
    for i in range(n):
        cat = cats[i % len(cats)]
        lo, hi = CATEGORIAS[cat]
        produtos.append({"product_id": f"P{i:04d}", "category": cat,
                         "price": round(rng.uniform(lo, hi), 2)})
    return produtos


class Gerador:
    def __init__(self, fora_de_ordem, n_usuarios=500):
        self.fora_de_ordem = fora_de_ordem
        self.produtos = criar_catalogo()
        # popularidade tipo "cauda longa": poucos produtos concentram as visitas
        self.pesos = [1.0 / (i + 1) ** 0.8 for i in range(len(self.produtos))]
        self.usuarios = [{
            "user_id": f"U{i:05d}",
            "city": random.choice(CIDADES),
            "session_id": uuid.uuid4().hex[:12],
            "carrinho": [],
        } for i in range(n_usuarios)]
        self.pedidos_abertos = {}
        self.em_alta = random.choice(self.produtos)
        self.proxima_troca = time.time() + 120

    # ------------------------------------------------------------------ util
    def escolher_produto(self):
        if random.random() < 0.25:
            return self.em_alta
        return random.choices(self.produtos, weights=self.pesos, k=1)[0]

    def evento(self, tipo, u, produto=None, quantidade=None,
               order_id=None, order_total=None, status=None):
        event_time = int(time.time() * 1000)
        if random.random() < self.fora_de_ordem:
            event_time -= random.randint(2_000, 30_000)  # chega atrasado
        return {
            "event_id": uuid.uuid4().hex,
            "event_type": tipo,
            "user_id": u["user_id"],
            "session_id": u["session_id"],
            "city": u["city"],
            "product_id": produto["product_id"] if produto else None,
            "category": produto["category"] if produto else None,
            "price": produto["price"] if produto else None,
            "quantity": quantidade,
            "order_id": order_id,
            "order_total": order_total,
            "delivery_status": status,
            "event_time": event_time,
            "event_time_iso": datetime.fromtimestamp(event_time / 1000, tz=timezone.utc)
                                      .isoformat(timespec="milliseconds"),
        }

    # --------------------------------------------------------------- ações
    def finalizar_compra(self, u):
        order_id = "O" + uuid.uuid4().hex[:10]
        total = round(sum(p["price"] * q for p, q in u["carrinho"]), 2)
        eventos = [self.evento("purchase", u, p, q, order_id, total) for p, q in u["carrinho"]]
        self.pedidos_abertos[order_id] = {"u": u, "etapa": -1, "atrasou": False}
        u["carrinho"] = []
        return eventos

    def atualizar_entrega(self):
        order_id = random.choice(list(self.pedidos_abertos))
        p = self.pedidos_abertos[order_id]
        if p["etapa"] >= 2 and not p["atrasou"] and random.random() < 0.10:
            status = "atrasado"
            p["atrasou"] = True
        else:
            p["etapa"] += 1
            status = FLUXO_ENTREGA[p["etapa"]]
            if status == "entregue":
                del self.pedidos_abertos[order_id]
        return self.evento("delivery_update", p["u"], order_id=order_id, status=status)

    def passo(self):
        if time.time() > self.proxima_troca:  # novo produto "viral" -> trend muda
            self.em_alta = random.choice(self.produtos)
            self.proxima_troca = time.time() + random.randint(90, 180)

        if self.pedidos_abertos and random.random() < 0.15:
            return [self.atualizar_entrega()]

        u = random.choice(self.usuarios)
        if random.random() < 0.03:
            u["session_id"] = uuid.uuid4().hex[:12]

        r = random.random()
        if u["carrinho"] and r < 0.06:
            return self.finalizar_compra(u)
        if u["carrinho"] and r < 0.09:
            p, q = u["carrinho"].pop(random.randrange(len(u["carrinho"])))
            return [self.evento("remove_from_cart", u, p, q)]
        if r < 0.25:
            p, q = self.escolher_produto(), random.randint(1, 3)
            u["carrinho"].append((p, q))
            return [self.evento("add_to_cart", u, p, q)]
        return [self.evento("view", u, self.escolher_produto())]


def main():
    ap = argparse.ArgumentParser(description="Gerador de eventos de e-commerce (JSON lines)")
    ap.add_argument("--saida", default="/data/logs/events.log")
    ap.add_argument("--eps", type=float, default=20, help="eventos por segundo (média)")
    ap.add_argument("--fora-de-ordem", type=float, default=0.10,
                    help="fração de eventos com event_time atrasado (0 a 1)")
    ap.add_argument("--max-eventos", type=int, default=0, help="0 = infinito")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(os.path.abspath(args.saida)), exist_ok=True)
    g = Gerador(args.fora_de_ordem)
    total, pico_ate = 0, 0.0
    print(f"Gerando ~{args.eps} eventos/s em {args.saida} (Ctrl+C para parar)", flush=True)

    with open(args.saida, "a", encoding="utf-8", buffering=1) as f:
        try:
            while not args.max_eventos or total < args.max_eventos:
                agora = time.time()
                if agora > pico_ate and random.random() < 0.001:
                    pico_ate = agora + 20  # "flash sale": 20 s com 5x mais eventos
                    print(">> pico de acessos (flash sale) por 20 s", flush=True)
                eps = args.eps * (5 if agora < pico_ate else 1)

                for ev in g.passo():
                    f.write(json.dumps(ev, ensure_ascii=False) + "\n")
                    total += 1
                    if total % 1000 == 0:
                        print(f"{total} eventos gerados", flush=True)
                time.sleep(random.uniform(0.5, 1.5) / eps)
        except KeyboardInterrupt:
            pass
    print(f"Fim. {total} eventos gerados.")


if __name__ == "__main__":
    main()
