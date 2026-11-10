"""
PFO 3 - Rediseño como Sistema Distribuido (Cliente-Servidor)
Servidor: recibe tareas por socket TCP y las distribuye a workers.

Arquitectura (versión local de la del diagrama):
  - Hilo principal: acepta conexiones de clientes.
  - Un hilo por cliente: lee las tareas que llegan por el socket y las
    deja en la cola de mensajes.
  - Cola de mensajes (queue.Queue): cumple el papel de RabbitMQ.
  - Despachador: saca tareas de la cola y las reparte entre los workers
    con round-robin (el mismo criterio que usa un balanceador como Nginx).
  - Workers: cada uno tiene su propio pool de hilos (ThreadPoolExecutor),
    ejecuta la tarea y le devuelve el resultado al cliente que la pidió.

Protocolo: JSON por línea (un mensaje JSON terminado en '\n').
  Pedido:    {"id": 1, "tipo": "factorial", "datos": 10}
  Respuesta: {"id": 1, "estado": "ok", "resultado": 3628800,
              "worker": "worker-2", "duracion_seg": 0.0001}
"""
import argparse
import itertools
import json
import math
import queue
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor


# ------------------------------------------------------------------ Tareas
def _primos_hasta(n):
    n = int(n)
    if not 0 <= n <= 1_000_000:
        raise ValueError("n debe estar entre 0 y 1.000.000")
    if n < 2:
        return 0
    criba = bytearray([1]) * (n + 1)
    criba[0] = criba[1] = 0
    for i in range(2, int(n ** 0.5) + 1):
        if criba[i]:
            criba[i * i :: i] = bytearray(len(criba[i * i :: i]))
    return sum(criba)


def _factorial(n):
    n = int(n)
    if not 0 <= n <= 1000:
        raise ValueError("n debe estar entre 0 y 1000")
    return str(math.factorial(n))  # como texto: puede ser un número enorme


def _esperar(segundos):
    segundos = float(segundos)
    if not 0 <= segundos <= 10:
        raise ValueError("los segundos deben estar entre 0 y 10")
    time.sleep(segundos)  # simula una tarea lenta (I/O, consulta a BD, etc.)
    return f"esperé {segundos} s"


TAREAS = {
    "eco": lambda d: str(d),
    "mayusculas": lambda d: str(d).upper(),
    "contar_palabras": lambda d: len(str(d).split()),
    "suma": lambda d: sum(float(x) for x in d),
    "factorial": _factorial,
    "primos": _primos_hasta,
    "esperar": _esperar,
}


# ----------------------------------------------------------------- Clientes
class ConexionCliente:
    """Envuelve el socket de un cliente. Varios hilos pueden responderle a la
    vez, así que el envío se protege con un lock."""

    def __init__(self, sock, direccion):
        self.sock = sock
        self.direccion = f"{direccion[0]}:{direccion[1]}"
        self._lock = threading.Lock()
        self.activa = True

    def enviar(self, mensaje):
        datos = (json.dumps(mensaje, ensure_ascii=False) + "\n").encode("utf-8")
        with self._lock:
            if not self.activa:
                return
            try:
                self.sock.sendall(datos)
            except OSError:
                self.activa = False


# ------------------------------------------------------------------ Workers
class Worker:
    """Un servidor worker con su propio pool de hilos."""

    def __init__(self, nombre, hilos):
        self.nombre = nombre
        self.pool = ThreadPoolExecutor(max_workers=hilos, thread_name_prefix=nombre)
        self.procesadas = 0
        self._lock = threading.Lock()

    def recibir(self, tarea, cliente):
        self.pool.submit(self._ejecutar, tarea, cliente)

    def _ejecutar(self, tarea, cliente):
        inicio = time.perf_counter()
        respuesta = {"id": tarea.get("id"), "worker": self.nombre}
        try:
            funcion = TAREAS[tarea["tipo"]]
            respuesta["resultado"] = funcion(tarea.get("datos"))
            respuesta["estado"] = "ok"
        except KeyError:
            respuesta["estado"] = "error"
            respuesta["error"] = f"tipo de tarea desconocido: {tarea.get('tipo')!r}"
        except Exception as e:  # datos inválidos, etc.
            respuesta["estado"] = "error"
            respuesta["error"] = str(e)
        respuesta["duracion_seg"] = round(time.perf_counter() - inicio, 4)

        with self._lock:
            self.procesadas += 1
        log(f"{self.nombre} terminó tarea {respuesta['id']} de {cliente.direccion} "
            f"({respuesta['estado']})")
        cliente.enviar(respuesta)

    def apagar(self):
        self.pool.shutdown(wait=False, cancel_futures=True)


# ------------------------------------------------------------------ Servidor
_log_lock = threading.Lock()


def log(texto):
    with _log_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {texto}", flush=True)


class Servidor:
    def __init__(self, host, puerto, cant_workers, hilos_por_worker):
        self.host = host
        self.puerto = puerto
        self.cola = queue.Queue()  # cola de mensajes (rol de RabbitMQ)
        self.workers = [Worker(f"worker-{i + 1}", hilos_por_worker)
                        for i in range(cant_workers)]
        self._turno = itertools.cycle(self.workers)  # round-robin
        self._corriendo = threading.Event()

    # Despachador: cola -> workers
    def _despachar(self):
        while self._corriendo.is_set():
            try:
                tarea, cliente = self.cola.get(timeout=0.5)
            except queue.Empty:
                continue
            worker = next(self._turno)
            log(f"tarea {tarea.get('id')} ({tarea.get('tipo')}) -> {worker.nombre}")
            worker.recibir(tarea, cliente)

    # Un hilo por cliente: socket -> cola
    def _atender_cliente(self, sock, direccion):
        cliente = ConexionCliente(sock, direccion)
        log(f"cliente conectado: {cliente.direccion}")
        try:
            with sock, sock.makefile("r", encoding="utf-8") as entrada:
                for linea in entrada:
                    linea = linea.strip()
                    if not linea:
                        continue
                    try:
                        tarea = json.loads(linea)
                        if not isinstance(tarea, dict) or "tipo" not in tarea:
                            raise ValueError
                    except ValueError:
                        cliente.enviar({"id": None, "estado": "error",
                                        "error": "mensaje inválido: se espera JSON con 'tipo'"})
                        continue
                    self.cola.put((tarea, cliente))
        except OSError:
            pass
        finally:
            cliente.activa = False
            log(f"cliente desconectado: {cliente.direccion}")

    def iniciar(self):
        self._corriendo.set()
        threading.Thread(target=self._despachar, name="despachador", daemon=True).start()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                srv.bind((self.host, self.puerto))
            except OSError:
                log(f"error: el puerto {self.puerto} ya está en uso. Cerrá el proceso "
                    "que lo usa o elegí otro con --puerto")
                self._corriendo.clear()
                return
            srv.listen()
            srv.settimeout(1.0)  # para poder cortar con Ctrl+C también en Windows
            hilos = self.workers[0].pool._max_workers
            log(f"servidor escuchando en {self.host}:{self.puerto} "
                f"con {len(self.workers)} workers de {hilos} hilos cada uno")
            log("Ctrl+C para detener")
            try:
                while True:
                    try:
                        sock, direccion = srv.accept()
                    except socket.timeout:
                        continue
                    threading.Thread(target=self._atender_cliente,
                                     args=(sock, direccion), daemon=True).start()
            except KeyboardInterrupt:
                log("deteniendo servidor...")
            finally:
                self._corriendo.clear()
                for w in self.workers:
                    log(f"{w.nombre}: {w.procesadas} tareas procesadas")
                    w.apagar()


def main():
    p = argparse.ArgumentParser(description="Servidor de tareas distribuidas (PFO 3)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--puerto", type=int, default=5000)
    p.add_argument("--workers", type=int, default=3, help="cantidad de workers")
    p.add_argument("--hilos", type=int, default=2, help="hilos por worker")
    args = p.parse_args()
    Servidor(args.host, args.puerto, args.workers, args.hilos).iniciar()


if __name__ == "__main__":
    main()
